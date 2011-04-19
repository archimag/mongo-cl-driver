;;;; connection.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver.wire)

(defclass connection ()
  ((hostname :initarg :hostname :initform "localhost" :reader connection-hostname)
   (port :initarg :port :initform 27017 :reader connection-port)
   (socket :initform nil :reader connection-socket)))

(defmethod shared-initialize :after ((conn connection) slot-names &key)
  (let ((socket (iolib.sockets:make-socket :connect :active
                                           :address-family :internet
                                           :type :stream)))
    (iolib.sockets:connect socket
                           (iolib.sockets:lookup-hostname (connection-hostname conn))
                           :port (connection-port conn))
    (setf (slot-value conn 'socket)
          socket)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; foreign bucket
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defvar *bucket-pool* nil)

(defconstant +bucket-size+ 4096)

(defun get-bucket ()
  (or (pop *bucket-pool*)
      (make-array +bucket-size+ :element-type '(unsigned-byte 8))))

(defun free-bucket (bucket)
  (push bucket *bucket-pool*))

(defclass bucket-target ()
  ((buckets :initform (make-array 0 :fill-pointer 0 :adjustable t))
   (bucket-index :initform -1)
   (pos-in-bucket :initform 0)))
  
(defmethod shared-initialize :after ((obj bucket-target) slot-names &key)
  (bucket-target-extend obj))

(defun bucket-target-extend (obj)
  (vector-push-extend (get-bucket)
                      (slot-value obj 'buckets))
  (incf (slot-value obj 'bucket-index))
  (setf (slot-value obj 'pos-in-bucket) 0))

(defun bucket-target-clear (obj)
  (iter (for bucket in-vector (slot-value obj 'buckets))
        (free-bucket bucket)))

(defun bucket-target-prepare-for-decode (obj)
  (setf (slot-value obj 'bucket-index) 0
        (slot-value obj 'pos-in-bucket) 0))

(defmacro with-bucket-target (name &body body)
  `(let ((,name (make-instance 'bucket-target)))
     (unwind-protect
          (progn ,@body)
       (bucket-target-clear ,name))))

(defmacro btref (obj)
  `(aref (aref (slot-value ,obj 'buckets)
               (slot-value ,obj 'bucket-index))
         (slot-value ,obj 'pos-in-bucket)))

(defmethod decode-byte ((source bucket-target))
  (prog1
      (btref source)
    (incf (slot-value source 'pos-in-bucket))
    (when (= (slot-value source 'pos-in-bucket) +bucket-size+)
      (incf (slot-value source 'bucket-index))
      (setf (slot-value source 'pos-in-bucket) 0))))

(defmethod encode-byte (byte (target bucket-target))
  (setf (btref target)
        byte)
  (incf (slot-value target 'pos-in-bucket))
  (when (= (slot-value target 'pos-in-bucket) +bucket-size+)
    (bucket-target-extend target)))

(defmethod bson-target-replace ((target bucket-target) sequence start)
  (iter (for i from start)
        (for j from 0 below (length sequence))
        (multiple-value-bind (index pos) (floor i +bucket-size+)
          (setf (aref (aref (slot-value target 'buckets)
                            index)
                      pos)
                (elt sequence j)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; 
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun send-msg (conn msg &aux (socket (connection-socket conn)))
  (with-bucket-target target
    (encode-protocol-message msg target)
    (iter (for i from 0 below (1- (slot-value target 'bucket-index)))
          (iolib.sockets:send-to socket
                                 (aref (slot-value target 'buckets)
                                       i)))
    (iolib.sockets:send-to socket
                           (aref (slot-value target 'buckets)
                                 (slot-value target 'bucket-index))
                           :start 0
                           :end (slot-value target 'pos-in-bucket))))

(defparameter *event-base* (make-instance 'iolib.multiplex:event-base))

(bt:make-thread #'(lambda ()
                    (iolib.multiplex:event-dispatch *event-base*))
                :name "Event-Loop Thread")

(defun read-reply (conn &optional callback &aux (socket (connection-socket conn)))
  (declare (optimize (debug 3)))
  (let ((tbucket (make-instance 'bucket-target))
        (size nil)
        (done-p nil))
    (labels ((decode-bucket ()
               (setf (slot-value tbucket 'bucket-index) 0
                     (slot-value tbucket 'pos-in-bucket) 0)
               (unwind-protect
                    (decode-op-reply tbucket)
                 (bucket-target-clear tbucket)))
             
             (read-bucket (fd event errorp)
               (declare (ignore event errorp))
               (multiple-value-bind (buffer count)
                   (iolib.sockets:receive-from socket
                                               :buffer (aref (slot-value tbucket 'buckets) 0)
                                               :start (slot-value tbucket 'pos-in-bucket))
                 (declare (ignore buffer))
                 (incf (slot-value tbucket 'pos-in-bucket) count)

                 (when (and (not size)
                            (> (slot-value tbucket 'pos-in-bucket)
                               4))
                   (let ((*decoded-bytes-count* 0))
                     (setf size
                           (decode-int32 (replace (make-array 4
                                                              :element-type '(unsigned-byte 8))
                                                  (aref (slot-value tbucket 'buckets) 0))))))

                 (when (and size (= size (slot-value tbucket 'pos-in-bucket)))
                   (setf done-p t)
                   (iolib.multiplex:remove-fd-handlers *event-base*
                                                       fd
                                                       :read t)
                   (when callback
                     (funcall callback (decode-bucket)))))))
      (iolib.multiplex:with-event-base (*event-base*)
        (iolib.multiplex:set-io-handler *event-base*
                                        (iolib.sockets:socket-os-fd socket)
                                        :read #'read-bucket)
        (unless callback
          (iter (while (not done-p))
                (iolib.multiplex:event-dispatch *event-base* :one-shot t))
          (decode-bucket))))))

(defun test ()
  (let ((conn (make-instance 'connection)))
    (unwind-protect
         (progn
           (send-msg conn
                     (make-instance 'op-query 
                                    :full-collection-name "test.thing"
                                    :return-field-selector nil))
           (finish-output (connection-socket conn))
           (read-reply conn
;;                       #'print
                       ))
      (close (connection-socket conn)))))


    
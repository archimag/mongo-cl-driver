;;;; connection.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver.wire)

(defparameter *event-base* (make-instance 'iolib.multiplex:event-base))

(bt:make-thread #'(lambda ()
                    (iolib.multiplex:event-dispatch *event-base*))
                :name "Event-Loop Thread")

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


(defun send-message (conn msg &optional callback)
  (let ((socket (connection-socket conn))
        (brigade (make-instance 'brigade))
        (bytes-sent 0)
        (done-p nil))
    
    (encode-protocol-message msg brigade)
    
    (flet ((send-brigade (fd event errorp)
             (declare (ignore event errorp))
             (incf bytes-sent
                   (iolib.sockets:send-to socket
                                          (aref (brigade-buckets brigade)
                                                (slot-value brigade 'bucket-index))
                                          :start bytes-sent
                                          :end (slot-value brigade 'pos-in-bucket)))
             (when (= bytes-sent (slot-value brigade 'pos-in-bucket))
               (iolib.multiplex:remove-fd-handlers *event-base*
                                                   fd
                                                   :write t)
               (setf done-p t)
               (brigade-free-buckets brigade)
               (when callback (funcall callback)))))      
      (cond
        (callback
         (iolib.multiplex:set-io-handler *event-base*
                                        (iolib.sockets:socket-os-fd socket)
                                        :write #'send-brigade))
        (t
         (iolib.multiplex:with-event-base (*event-base*)
           (iolib.multiplex:set-io-handler *event-base*
                                           (iolib.sockets:socket-os-fd socket)
                                           :write #'send-brigade)
           (iter (while (not done-p))
                 (iolib.multiplex:event-dispatch *event-base* :one-shot t)))))
      (values))))
                      

(defun read-reply (conn &optional callback &aux (socket (connection-socket conn)))
  (declare (optimize (debug 3)))
  (let ((brigade (make-instance 'brigade))
        (size nil)
        (done-p nil))
    (labels ((decode-brigade ()
               (setf (slot-value brigade 'bucket-index) 0
                     (slot-value brigade 'pos-in-bucket) 0)
               (unwind-protect
                    (decode-op-reply brigade)
                 (brigade-free-buckets brigade)))
             
             (read-brigade (fd event errorp)
               (declare (ignore event errorp))
               (multiple-value-bind (buffer count)
                   (iolib.sockets:receive-from socket
                                               :buffer (aref (brigade-buckets brigade) 0)
                                               :start (slot-value brigade 'pos-in-bucket))
                 (declare (ignore buffer))
                 (incf (slot-value brigade 'pos-in-bucket) count)

                 (when (and (not size)
                            (> (slot-value brigade 'pos-in-bucket)
                               4))
                   (let ((*decoded-bytes-count* 0))
                     (setf size
                           (decode-int32 (replace (make-array 4
                                                              :element-type '(unsigned-byte 8))
                                                  (aref (brigade-buckets brigade) 0))))))

                 (when (and size (= size (slot-value brigade 'pos-in-bucket)))
                   (iolib.multiplex:remove-fd-handlers *event-base*
                                                       fd
                                                       :read t)
                   (setf done-p t)
                   (when callback
                     (funcall callback
                              (unwind-protect
                                   (decode-brigade)
                                (brigade-free-buckets brigade))))))))
      (cond
        (callback
         (iolib.multiplex:set-io-handler *event-base*
                                        (iolib.sockets:socket-os-fd socket)
                                        :read #'read-brigade)
         (values))
        (t
         (iolib.multiplex:with-event-base (*event-base*)
           (iolib.multiplex:set-io-handler *event-base*
                                           (iolib.sockets:socket-os-fd socket)
                                           :read #'read-brigade)
           (iter (while (not done-p))
                 (iolib.multiplex:event-dispatch *event-base* :one-shot t))
           (decode-brigade)))))))

(defparameter *connection* (make-instance 'connection))

(defun test (&optional callback)
  (labels ((getreply ()
               (read-reply *connection* callback))
           (sendmsg ()
             (send-message *connection*
                           (make-instance 'op-query 
                                          :full-collection-name "test.things"
                                          :return-field-selector nil)
                           (if callback #'getreply))
             (unless callback
               (getreply))))
    (sendmsg)))
    
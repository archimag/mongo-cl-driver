;;;; connection.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver.wire)

(defparameter *read-timeout* 20)
(defparameter *write-timeout* 20)

(defparameter *event-base* (make-instance 'iolib.multiplex:event-base))

(defun safe-funcall (function &rest args)
  (handler-case (apply function args)
    (error (condition)
      (bordeaux-threads:make-thread
       (alexandria:named-lambda show-error-lambda ()
         (declare (optimize (debug 3)))
         (error condition))))))

(defun safe-fd-handler (handler &optional error-callback)
  (alexandria:named-lambda safe-handler-lambda (fd event errorp)
    (declare (ignore event))
    (labels ((handle-error (err)
               (iolib.multiplex:remove-fd-handlers *event-base* fd :write t :error t :read t)
               (cond
                 (error-callback
                  (safe-funcall error-callback err nil)
                  (return-from safe-handler-lambda))
                 (t (error err)))))

      (when errorp
        (ignore-errors
          (iolib.syscalls:close fd))
        (handle-error errorp))

      (handler-case (funcall handler)
        (error (condition)
          (handle-error condition))))))
      
(defun send-message (socket msg &key async callback)
  (let* ((brigade (encode-protocol-message msg :brigade))
         (total-size (brigade-total-size brigade))
         (bytes-sent 0)
         (done-p nil))
    (labels ((send-brigade ()
               (multiple-value-bind (bucket start end) (find-bucket brigade bytes-sent)
                 (incf bytes-sent
                       (iolib.sockets:send-to socket bucket :start start :end end)))
               
               (when (= bytes-sent total-size)
                 (iolib.multiplex:remove-fd-handlers *event-base*
                                                     (iolib.sockets:socket-os-fd socket)
                                                     :write t)
                 (setf done-p t)
                 (brigade-free-buckets brigade)

                 (when async
                   (safe-funcall callback nil nil)))))
      (cond
        (async
         (iolib.multiplex:set-io-handler *event-base*
                                        (iolib.sockets:socket-os-fd socket)
                                        :write (safe-fd-handler #'send-brigade callback)
                                        :timeout *write-timeout*))
        (t
         (iolib.multiplex:with-event-base (*event-base*)
           (iolib.multiplex:set-io-handler *event-base*
                                           (iolib.sockets:socket-os-fd socket)
                                           :write (safe-fd-handler #'send-brigade)
                                           :timeout *write-timeout*)
           (iter (while (not done-p))
                 (iolib.multiplex:event-dispatch *event-base* :one-shot t)))))
      
      (values))))

(defun read-reply (socket &key async callback)
  (declare (optimize (debug 3)))
  (let ((brigade (make-instance 'brigade))
        (size nil)
        (result nil))
    (labels ((decode-brigade ()
               (brigade-prepare-for-read brigade)
               (unwind-protect
                    (decode-op-reply brigade)
                 (brigade-free-buckets brigade)))

             (receive-bucket (brigade)
               (multiple-value-bind (bucket start) (active-bucket brigade)
                 (iolib.sockets:receive-from socket :buffer bucket :start start)))
             
             (read-brigade ()
               (multiple-value-bind (buffer count) (receive-bucket brigade)
                 (when (and (not size) (> count 4))
                   (let ((*decoded-bytes-count* 0))
                     (setf size
                           (decode-int32 (replace (make-array 4 :element-type 'ub8)
                                                  buffer)))))
                 
                 (brigade-shift brigade count)
                 
                 (when (and size (= size (brigade-total-size brigade)))
                   (iolib.multiplex:remove-fd-handlers *event-base*
                                                       (iolib.sockets:socket-os-fd socket)
                                                       :read t)
                   (setf result (decode-brigade))
                   (when async
                     (safe-funcall callback nil result))))))
      (cond
        (async
         (iolib.multiplex:set-io-handler *event-base*
                                        (iolib.sockets:socket-os-fd socket)
                                        :read (safe-fd-handler #'read-brigade callback)
                                        :timeout *read-timeout*)
         (values))
        (t
         (iolib.multiplex:with-event-base (*event-base*)
           (iolib.multiplex:set-io-handler *event-base*
                                           (iolib.sockets:socket-os-fd socket)
                                           :read (safe-fd-handler #'read-brigade)
                                           :timeout *read-timeout*)
           (iter (while (not result))
                 (iolib.multiplex:event-dispatch *event-base* :one-shot t))
           result))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; interface
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(bt:make-thread (alexandria:named-lambda mongodb-event-loop-lambda ()
                  (iolib.multiplex:event-dispatch *event-base*))
                :name "MongoDB event loop thread")

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

(defun close-connection (connection)
  (close (connection-socket connection)))

(defun send-message-sync (connection message)
  (send-message (connection-socket connection)
                message))

(defun send-message-async (connection message &optional callback)
  (send-message (connection-socket connection)
                message
                :async t
                :callback callback))

(defun read-reply-sync (connection)
  (read-reply (connection-socket connection)))

(defun read-reply-async (connection callback)
  (read-reply (connection-socket connection)
              :async t
              :callback callback))


(defun send-and-read-sync (connection message)
  (let ((socket (connection-socket connection)))
    (send-message socket message)
    (read-reply socket)))

(defun send-and-read-async (connection message callback)
  (let ((socket (connection-socket connection)))
    (labels ((read-and-call (errorp _)
               (declare (ignore _))
               (cond
                 (errorp (funcall callback errorp nil))
                 (t (read-reply socket
                                :async t
                                :callback callback)))))
    (send-message socket
                  message
                  :async t
                  :callback #'read-and-call))))


;;;; connection.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver.wire)

(defclass connection ()
  ((host :initarg :host :initform (coerce #(127 0 0 1) '(simple-array (unsigned-byte 8) (4))) :reader connection-host)
   (port :initarg :port :initform 27017 :reader connection-port)
   (socket :initform nil :reader connection-socket)))

(defmethod shared-initialize :after ((conn connection) slot-names &key)
  (let ((socket (iolib.sockets:make-socket :connect :active
                                           :address-family :internet
                                           :type :stream)))
    (iolib.sockets:connect socket
                           (make-instance 'iolib.sockets:inet-address
                                          :name (connection-host conn))
                           :port (connection-port conn))
    (setf (slot-value conn 'socket)
          socket)))

    
(defun send-msg (conn msg)
  (write-sequence (encode-protocol-message msg :vector)
                  (connection-socket conn)))

(defun read-reply (conn)
  (let* ((*decoded-bytes-count* 0)
         (size (decode-int32 (connection-socket conn)))
         (octets (make-array size
                             :initial-element 0
                             :element-type '(unsigned-byte 8))))
    (read-sequence octets
                   (connection-socket conn)
                   :start 4)
    (decode-op-reply octets)))


(defun test ()
  (let ((conn (make-instance 'connection)))
    (unwind-protect
         (progn
           (send-msg conn
                     (make-instance 'op-query 
                                    :full-collection-name "test.things"
                                    :return-field-selector nil))
           (finish-output (connection-socket conn))
           (op-reply-documents (read-reply conn)))
      (close (connection-socket conn)))))
    
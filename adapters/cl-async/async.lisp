;;; connection.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(defpackage #:mongo-cl-driver.cl-async
  (:nicknames #:mongo.as)
  (:use #:cl #:iter #:mongo-cl-driver.adapters #:mongo-cl-driver.wire #:mongo-cl-driver.bson #:cl-async #:cl-async-future)
  (:import-from #:mongo-cl-driver.bson #:ub8)
  (:import-from #:alexandria #:named-lambda)
  (:shadowing-import-from #:cl-async-future #:finish)
  (:export #:mongo-client))

(in-package #:mongo-cl-driver.cl-async)

(defclass mongo-client (mongo-cl-driver:mongo-client)
  ((socket :initform nil :accessor mongo-client-socket)))

(defun signal-connection-error (future err)
  (signal-error future
                (make-condition 'mongo-cl-driver:connection-failure
                                :description err)))

(defmethod mongo-cl-driver:create-mongo-client ((adapter (eql :cl-async)) &key write-concern server)
  (let ((future (make-future))
        (client (make-instance 'mongo-client :write-concern write-concern :server server)))
    (labels ((on-connect-handler (socket)
               (declare (ignore socket))
               (finish future client))
             (on-error-handler (err)
               (signal-connection-error future err)
               (finish future)))
      (setf (mongo-client-socket client)
            (tcp-connect (mongo-cl-driver:server-hostname client)
                         (mongo-cl-driver:server-port client)
                         nil
                         #'on-error-handler
                         :connect-cb #'on-connect-handler))
      future)))

(defmethod mongo-client-close ((mongo-client mongo-client))
  (close-socket (mongo-client-socket mongo-client)))

(defmethod send-message ((client mongo-client) message &key write-concern)
  (let* ((future (make-future))
         (crazy-p (eql (mongo-cl-driver:write-concern-w write-concern) :errors-ignored)))
    (labels ((error-send-message-callback (err)
               (signal-connection-error future err))
             (send-message-callback (socket)
               (declare (ignore socket))
               (finish future)))
      (write-socket-data (mongo-client-socket client)
                         (encode-protocol-message message :vector)
                         :write-cb (if (not crazy-p) #'send-message-callback)
                         :event-cb (if (not crazy-p) #'error-send-message-callback)))
    future))

(defmethod send-message-and-read-reply ((mongo-client mongo-client) message)
  (let ((future (make-future))
        (reply-vector (make-array 0 :fill-pointer 0 :adjustable t :element-type 'ub8)))
    (labels ((error-read-reply-callback (err)
               (signal-error future
                             (make-condition 'mongo-cl-driver:connection-failure
                                             :description err)))
             
             (read-reply-callback (socket data)
               (declare (ignore socket))
               (iter (for byte in-vector data)
                     (vector-push-extend byte reply-vector))
               (when (> (length reply-vector) 4)
                 (let ((*decoded-bytes-count* 0))
                   (when (= (decode-int32 (replace (make-array 4 :element-type 'ub8) reply-vector))
                            (fill-pointer reply-vector))
                     (let* ((reply (decode-op-reply reply-vector))
                            (condition (check-reply-last-error reply)))
                       (if condition
                           (signal-error future condition)
                           (finish future reply))))))))
      
      (write-socket-data (mongo-client-socket mongo-client)
                         (encode-protocol-message message :vector)
                         :event-cb #'error-read-reply-callback
                         :read-cb #'read-reply-callback)
      future)))

    


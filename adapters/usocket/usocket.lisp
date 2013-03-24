;;;; usocket.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(defpackage #:mongo-cl-driver.adapters.usocket
  (:nicknames #:mongo.usocket)
  (:use #:cl #:mongo-cl-driver.adapters #:mongo-cl-driver.wire #:cl-async-future)
  (:import-from #:mongo-cl-driver.bson #:ub8)
  (:export #:mongo-client))

(in-package #:mongo-cl-driver.adapters.usocket)

(defclass mongo-client (mongo-cl-driver:mongo-client)
  ((socket :initform nil :accessor mongo-client-socket)))

(defmethod shared-initialize :after ((client mongo-client) slot-names &key)
  (setf (mongo-client-socket client)
        (usocket:socket-connect (mongo-cl-driver:server-hostname client)
                                (mongo-cl-driver:server-port client)
                                :protocol :stream
                                :element-type 'ub8)))

(defmethod mongo-cl-driver:create-mongo-client ((adapter (eql :usocket)) &key write-concern server)
  (make-instance 'mongo-client :write-concern write-concern :server server))

(defmethod mongo-client-close ((mongo-client mongo-client))
  (usocket:socket-close (mongo-client-socket mongo-client)))

(defmethod send-message ((client mongo-client) msg &key write-concern)
  (declare (ignore write-concern))
  (let ((socket (usocket:socket-stream (mongo-client-socket client))))
    (write-sequence (encode-protocol-message msg :vector) socket)
    (finish-output socket)))

(defmethod send-message-and-read-reply ((client mongo-client) msg)
  (send-message client msg)
  (let* ((reply (decode-op-reply (usocket:socket-stream (mongo-client-socket client))))
         (condition (check-reply-last-error reply)))
    (when condition
      (error condition))
    reply))
    



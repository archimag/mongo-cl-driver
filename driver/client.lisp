;;;; write-concern.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Write Concern
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defgeneric write-concern (obj)
  (:documentation "Return Write Concern for OBJ"))

(defclass write-concern ()
  ((w :initform :receipt-acknowledged :reader write-concern-w)
   (j :initarg :j :initform nil :accessor write-concern-j)
   (fsync :initarg :fsync :initform nil :accessor write-concern-fsync)
   (wtimeout :initarg :wtimeout :initform nil :accessor write-concern-wtimeout)))

(defmethod shared-initialize :after ((wc write-concern) slot-names &key (w :receipt-acknowledged w-supplied-p)  &allow-other-keys)
  (when w-supplied-p
    (setf (write-concern-w wc) w)))

(defun (setf write-concern-w) (w wc)
  (check-type w (or (member :errors-ignored :unacknowledged :receipt-acknowledged)
                            (integer 2)))
  (setf (slot-value wc 'w) w))

(defun write-concern-options (wc)
  (let ((doc (make-hash-table :test 'equal))
        (w (write-concern-w wc))
        (j (write-concern-j wc))
        (fsync (write-concern-fsync wc))
        (wtimeout (write-concern-wtimeout wc)))
    (when (integerp w)
      (setf (gethash "w" doc) w))
    (when j
      (setf (gethash "j" doc) j))
    (when fsync
      (setf (gethash "fsync" doc) fsync))
    (when wtimeout
      (setf (gethash "wtimeout" doc) wtimeout))
    doc))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Server Config
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defclass server-config ()
  ((hostname :initarg :hostname :initform nil :reader server-hostname)
   (port :initarg :port :initform nil :reader server-port)))

(defmethod shared-initialize :after ((server server-config) slot-names &key)
  (with-slots (hostname port) server
    (unless hostname
      (setf hostname "localhost"))
    (unless port
      (setf port 27017))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; MongoClient
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defclass mongo-client ()
  ((write-concern :initarg :write-concern :initform nil :accessor write-concern)
   (server-config :initarg :server :initform nil :reader server-config)))

(defmethod shared-initialize :after ((client mongo-client) slot-names &key)
  (with-slots (write-concern server-config) client
    (unless write-concern
      (setf write-concern
            (make-instance 'write-concern)))
    (unless server-config
      (setf server-config
            (make-instance 'server-config)))))

(defmethod server-hostname ((client mongo-client))
  (server-hostname (server-config client)))

(defmethod server-port ((client mongo-client))
  (server-port (server-config client)))

(defgeneric create-mongo-client (adapter &key write-concern server &allow-other-keys)
  (:documentation "Make MongoClient"))

(defmacro with-client ((client form) &body body)
  `(let ((,client ,form))
     (unwind-protect
          (progn ,@body)
       (mongo-client-close ,client))))

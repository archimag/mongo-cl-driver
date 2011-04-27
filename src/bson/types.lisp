;;;; types.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver.bson)

(deftype ub8 () '(unsigned-byte 8))
(deftype ub8-sarray (&optional (size '*))
  `(simple-array ub8 (,size)))
(deftype ub8-vector (&optional (size '*))
  `(vector ub8 ,size))

;;; Min and Max keys

(defconstant +min-key+ '+min-key+)
(defconstant +max-key+ '+max-key+)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; ObjectId
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defclass object-id ()
  ((raw :initform (make-array 12 :element-type '(unsigned-byte 8)))))

(defmethod shared-initialize :after ((id object-id) slot-names &key raw)
  (when raw
    (replace (slot-value id 'raw) raw)))

(defmethod print-object ((id object-id) stream)
  (print-unreadable-object (id stream :type t :identity nil)
    (iter (for ch in-vector (slot-value id 'raw))
          (format stream "~2,'0X" ch))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Binary data
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defclass binary-data ()
  ((subtype :initarg :subtype :initform :generic :accessor binary-data-subtype)
   (octets :initarg :octets :initform nil :accessor binary-data-octets)))

(defmethod shared-initialize :after ((data binary-data) slot-names &key &allow-other-keys)
  (check-type (binary-data-subtype data)
              (member :generic :function :uuid :md5 :user-defined))
  (check-type (binary-data-octets data) ub8-vector))

(defmethod print-object ((data binary-data) stream)
  (print-unreadable-object (data stream :type t :identity nil)
    (format stream
            "~A ~A bytes"
            (binary-data-subtype data)
            (length (binary-data-octets data)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Regular expressions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defclass regex ()
  ((pattern :initarg :pattern :initform "" :reader regex-pattern)
   (options :initarg :options :initform "" :reader regex-options)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; JavaScript w/ scope
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defclass javascript-w-scope ()
  ((code :initarg :code :reader javascript-code)
   (scope :initarg :scope :initform (make-hash-table :test 'equal) :reader javascript-scope)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; timestamp
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defclass mongo-timestamp ()
  ((value :initarg :value :reader mongo-timestamp-value)))


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

;;;; ObjectId

(defclass object-id ()
  ((raw :initform (make-array 12 :element-type '(unsigned-byte 8)))))

(defvar *object-id-inc* 0)

(defvar *object-id-inc-lock*
  (bordeaux-threads:make-lock "ObjectId inc lock"))

(defmethod shared-initialize :after ((id object-id) slot-names &key raw text)
  (cond
    (raw (assert (= (length raw) 12))
         (replace (slot-value id 'raw)
                  raw))

    (text (assert (= (length text) 24))
          (with-input-from-string (in text)
            (iter (repeat 12)
                  (for i from 0)
                  (setf (aref (slot-value id 'raw) i)
                        (parse-integer (format nil "~C~C" (read-char in) (read-char in)) :radix 16)))))
    
    (t (generate-id (slot-value id 'raw)))))

(defun generate-id (raw)
  (let ((time (local-time:timestamp-to-unix (local-time:now)))
        (hostname (ironclad:digest-sequence
                   :md5
                   (babel:string-to-octets (machine-instance) :encoding :utf-8)))
        (pid #+clisp (system::process-id)
             #+(and lispworks (or unix windows)) (system::getpid)
             #+(and sbcl unix) (sb-unix:unix-getpid)
             #+(and cmu unix) (unix:unix-getpid)
             #+openmcl (ccl::getpid)
             #-(or clisp (and lispworks (or unix windows)) (and sbcl unix) (and cmu unix) (and openmcl unix) openmcl)
             (error "Impossible to determine the PID"))
        (inc (bordeaux-threads:with-recursive-lock-held (*object-id-inc-lock*)
               (setf *object-id-inc*
                     (rem (1+ *object-id-inc*)
                          #xFFFFFF)))))

    ;; 4 byte current time
    (setf (aref raw 0) (ldb (byte 8 0) time)
          (aref raw 1) (ldb (byte 8 8) time)
          (aref raw 2) (ldb (byte 8 16) time)
          (aref raw 3) (ldb (byte 8 24) time))

    ;; 3 bytes machine
    (setf (aref raw 4) (aref hostname 0)
          (aref raw 5) (aref hostname 1)
          (aref raw 6) (aref hostname 2))
          
    ;; 2 bytes pid
    (setf (aref raw 7) (ldb (byte 8 0) pid)
          (aref raw 8) (ldb (byte 8 8) pid))

    ;; 3 bytes inc
    (setf (aref raw 9) (ldb (byte 8 0) inc)
          (aref raw 10) (ldb (byte 8 8) inc)
          (aref raw 11) (ldb (byte 8 16) inc))))

(defun format-object-id (id)
  (with-output-to-string (stream)
    (iter (for ch in-vector (slot-value id 'raw))
          (format stream "~2,'0X" ch))))

(defmethod print-object ((id object-id) stream)
  (print-unreadable-object (id stream :type t :identity nil)
    (format stream (format-object-id id))))

;;;; Binary data

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

;;;; Regular expressions

(defclass regex ()
  ((pattern :initarg :pattern :initform "" :reader regex-pattern)
   (options :initarg :options :initform "" :reader regex-options)))

;;;; JavaScript w/ scope

(defclass javascript ()
  ((code :initarg :code :reader javascript-code)
   (scope :initarg :scope :initform (make-hash-table :test 'equal) :reader javascript-scope)))

;;;; timestamp

(defclass mongo-timestamp ()
  ((value :initarg :value :reader mongo-timestamp-value)))

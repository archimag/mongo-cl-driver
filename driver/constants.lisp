;;;; constants.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver)

(defmacro defconstant (name value &optional doc)
  "Make sure VALUE is evaluated only once \(to appease SBCL).
Copied from Hunchentoot."
  `(cl:defconstant ,name (if (boundp ',name) (symbol-value ',name) ,value)
     ,@(when doc (list doc))))

;;;; Write Concern

(defconstant +write-concern-normal+
  (make-instance 'write-concern
                 :w :receipt-acknowledged)
  "Exceptions are raised for network issues, and server errors; waits on a server for the write operation")

(defconstant +write-concern-fsync+
  (make-instance 'write-concern
                 :w :receipt-acknowledged
                 :fsync t)
  "Exceptions are raised for network issues, and server errors and the write operation waits for the server to flush the data to disk")

(defconstant +write-concern-replicas+
  (make-instance 'write-concern :w 2)
  "Exceptions are raised for network issues, and server errors; waits for at least 2 servers for the write operation")

(defconstant +write-concern-journal+
  (make-instance 'write-concern
                 :w :receipt-acknowledged
                 :j t)
  "The mongod will confirm the write operation only after it has written the operation to the journal.")

(defconstant +write-concern-fast+
  (make-instance 'write-concern
                 :w :unacknowledged)
  "Exceptions are raised for network issues, but not server errors")

(defconstant +write-concern-crazy+
  (make-instance 'write-concern
                 :w :errors-ignored)
  "No exceptions are raised, even for network issues")

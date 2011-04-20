;;;; son-syntax-sugar.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver.son-syntax-sugar)

(defgeneric @property (obj name)
  (:documentation "Get OBJ property by NAME"))

(defgeneric (setf @property) (newvalue obj name)
  (:documentation "Set OBJ property NAME to NEWVALUE"))

(defmethod @property ((obj hash-table) name)
  (gethash name obj))

(defmethod (setf @property) (newvalue (obj hash-table) name)
  (setf (gethash name obj)
        newvalue))

(defmethod @property ((obj array) index)
  (check-type index integer)
  (aref obj index))

(defmethod (setf @property) (newvalue (obj array) index)
  (check-type index integer)
  (setf (aref obj index)
        newvalue))

(defmethod @property ((obj cons) name)
  (if (consp (car obj))
      (assoc name obj :test #'string=)
      (getf obj name)))

(defmethod @property (obj (name symbol))
  (slot-value obj name))

(defmethod (setf @property) (newvalue obj (name symbol))
  (setf (slot-value obj name)
        newvalue))

(defmacro @ (expr &rest accessors)
    (if accessors
        `(@ (@property ,expr (car ,accessors))
            (cdr ,accessors))
        expr))

    
(defun %ht (&rest args &key &allow-other-keys)
  (let ((ht (make-hash-table :test 'equal)))
    (iter (for item on args by #'cddr)
          (setf (gethash (first item) ht)
                (second item)))
    ht))
;;;; son-sugar.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver.son-sugar)

(defun son (&rest args)
  (when (oddp (length args))
    (error "odd number of &ARGS arguments"))
  (let ((son (make-hash-table :test 'equal)))
    (iter (for item on args by #'cddr)
          (for key = (first item))
          (setf (gethash (if (symbolp key)
                             (camel-case:lisp-to-camel-case (symbol-name (first item)))
                             key)
                         son)
                (second item)))
    son))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; pprint
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun print-son-hash-table (str ht)
  (format str
          "{~{~{~S : ~S~}~^, ~}}"
          (loop for key being the hash-keys of ht
             for value being the hash-values of ht
             collect (list key value))))

(defun use-son-printer ()
  (set-pprint-dispatch 'hash-table #'print-son-hash-table))

(defmacro with-son-printer (&body body)
  `(let ((*print-pprint-dispatch* (copy-pprint-dispatch)))
     (use-son-printer)
     ,@body))
  
(defun print-son (obj &optional stream)
  (with-son-printer
    (print obj stream)))
    
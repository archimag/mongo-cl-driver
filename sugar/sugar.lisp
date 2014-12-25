;;;; sugar.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(defpackage #:mongo-cl-driver.sugar
  (:nicknames #:mongo.sugar)
  (:use #:cl #:iter)
  (:export #:$ #:$.get #:$.set #:$.id
           #:use-son-printer
           #:with-son-printer
           #:print-son))

(in-package #:mongo-cl-driver.sugar)

(defun $ (&rest args)
  (when (oddp (length args))
    (error "odd number of &ARGS arguments"))
  (let ((son (make-hash-table :test 'equal)))
    (iter (for item on args by #'cddr)
          (for key = (first item))
          (setf (gethash key son)
                (second item)))
    son))

(defun $.get (object key)
  (gethash key object))

(defun $.set (object key value)
  (setf (gethash key object)
        value))

(defun $.id (object)
  (gethash "_id" object))


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
    


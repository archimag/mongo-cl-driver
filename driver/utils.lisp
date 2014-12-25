;;;; utils.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver)

(defun try-unpromisify (promise)
  (if (and (promisep promise) (promise-finished-p promise))
      (values-list (blackbird-base::promise-values promise))
      promise))
  
(defun check-collection-name (name)
    (check-type name string)
    (when (or (string= name "")
              (search ".." name))
      (error "collection names cannot be empty"))
    (when (and (cl:find #\$ name)
               (not (or (starts-with-subseq "oplog.$main" name)
                        (starts-with-subseq "$main" name))))
      (error "collection names must not contain '$': ~A" name))
    (when (or (char= (char name 0) #\.)
              (char= (char name (1- (length name))) #\.))
      (error "collection names must not start or end with '.': ~A" name))
    (when (cl:find (code-char 0) name)
      (error "collection names must not contain the null character")))

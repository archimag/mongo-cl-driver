;;;; suite.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(defpackage #:mongo-cl-driver.test
  (:use #:cl #:iter #:lift
        #:mongo-cl-driver.bson
        #:mongo-cl-driver.wire)
  (:export #:run-mongo-cl-driver-tests))

(in-package #:mongo-cl-driver.test)

(deftestsuite mogno-cl-driver-test () ())

(defun run-mongo-cl-driver-tests (&optional (test 'mogno-cl-driver-test))
  "Run all mongo-cl-driver tests"
  (run-tests :suite test :report-pathname nil))

(defmethod asdf:perform ((op asdf:test-op) (system (eql (asdf:find-system '#:mongo-cl-driver-test))))
  (let* ((test-results (run-mongo-cl-driver-tests))
         (errors (lift:errors test-results))
         (failures (lift:failures  test-results)))
    (if (or errors failures)
        (error "test-op failed: ~A"
               (concatenate 'list errors failures))
        (print test-results))))
  

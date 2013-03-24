;;;; package.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(defpackage #:mongo-cl-driver.iolib
  (:nicknames #:mongo.iolib)
  (:use #:cl #:iter #:mongo-cl-driver.adapters #:mongo-cl-driver.wire #:mongo-cl-driver.bson)
  (:export #:connection
           #:connection-event-base
           #:*read-timeout*
           #:*write-timeout*))

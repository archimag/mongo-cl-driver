;;;; package.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(defpackage #:mongo-cl-driver.adapters
  (:use #:cl)
  (:export #:mongo-client-close
           #:check-reply-last-error
           #:send-message
           #:send-message-and-read-reply))

(in-package #:mongo-cl-driver.adapters)

(defgeneric mongo-client-close (mongo-client)
  (:documentation "Close the current db connection, including all the child database instances."))

(defgeneric send-message (mongo-client message &key write-concern)
  (:documentation "Send a message"))

(defgeneric send-message-and-read-reply (mongo-client message)
  (:documentation "Send and read"))

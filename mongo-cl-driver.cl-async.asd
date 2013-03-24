;;;; mongo-cl-driver.usocket.asd
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(defsystem #:mongo-cl-driver.cl-async
  :depends-on (#:mongo-cl-driver #:cl-async)
  :pathname "adapters/cl-async"
  :components ((:file "async")))

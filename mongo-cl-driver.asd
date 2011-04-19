;;;; mongo-cl-driver.asd
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(defsystem #:mongo-cl-driver
    :depends-on (#:iterate #:babel #:ieee-floats #:camel-case #:closer-mop #:iolib.sockets
                 #:bordeaux-threads)
    :components
    ((:module "src"
              :components
              ((:file "packages")
               (:file "bson" :depends-on ("packages"))
               (:file "wire" :depends-on ("bson"))
               (:file "bucket-brigade" :depends-on ("bson"))
               (:file "connection" :depends-on ("wire" "bucket-brigade"))))))

(defsystem #:mongo-cl-driver-test
  :depends-on (#:mongo-cl-driver #:lift)
  :components ((:module "t"
                        :components
                        ((:file "suite")
                         (:file "bson" :depends-on ("suite"))))))

(defmethod perform ((o test-op) (c (eql (find-system '#:mongo-cl-driver))))
  (operate 'load-op '#:mongo-cl-driver)
  (operate 'test-op '#:mongo-cl-driver-test))


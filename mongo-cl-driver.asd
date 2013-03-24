;;;; mongo-cl-driver.asd
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(defsystem #:mongo-cl-driver
    :depends-on (#:iterate #:closer-mop #:local-time
                 #:babel #:ieee-floats #:ironclad
                 #:bordeaux-threads
                 #:cl-async-future)
    :serial t
    :components
    ((:module "bson"
              :serial t
              :components ((:file "package")
                           (:file "types")
                           (:file "encode")
                           (:file "decode")))
     (:module "wire"
              :serial t
              :components ((:file "package")
                           (:file "meta-protocol")
                           (:file "protocol"))
              :depends-on ("bson"))
     (:module "adapters"
              :components ((:file "adapters")))
     (:module "sugar"
              :components ((:file "sugar")))
     (:module "driver"
              :serial t
              :pathname "driver"
              :components ((:file "packages")
                           (:file "utils")
                           (:file "conditions")
                           (:file "client")
                           (:file "database")
                           (:file "collection")
                           (:file "cursor")
                           (:file "constants")
                           ))))

(defsystem #:mongo-cl-driver-test
  :depends-on (#:mongo-cl-driver #:lift)
  :components ((:module "t"
                        :components
                        ((:file "suite")
                         (:file "bson" :depends-on ("suite"))
                         (:file "wire" :depends-on ("suite"))))))

(defmethod perform ((o test-op) (c (eql (find-system '#:mongo-cl-driver))))
  (operate 'load-op '#:mongo-cl-driver)
  (operate 'test-op '#:mongo-cl-driver-test))

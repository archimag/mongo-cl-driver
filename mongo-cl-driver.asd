;;;; mongo-cl-driver.asd
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(defsystem #:mongo-cl-driver
    :depends-on (#:iterate #:closer-mop #:local-time
                 #:babel #:ieee-floats #:ironclad
                 #:bordeaux-threads #:iolib.sockets)
    :pathname "src/"
    :serial t
    :components
    ((:module "bson"
              :pathname "bson/"
              :serial t
              :components ((:file "package")
                           (:file "types")
                           (:file "encode")
                           (:file "decode")))
     (:module "wire"
              :pathname "wire/"
              :serial t
              :components ((:file "package")
                           (:file "bucket-brigade")
                           (:file "protocol")
                           (:file "connection"))
              :depends-on ("bson"))
     (:file "packages"  :depends-on ("wire"))
     (:file "database" :depends-on ("packages"))
     (:file "cursor" :depends-on ("packages"))
     (:file "collection" :depends-on ("cursor"))
     (:file "son-sugar" :depends-on ("packages"))))

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


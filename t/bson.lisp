;;;; bson.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver.test)

(deftestsuite mongo-bson-test (mogno-cl-driver-test) ())

(addtest (mongo-bson-test)
  encode-basic-test-1
  (ensure-same (encode () :list)
               '(#x05 #x00 #x00 #x00 #x00)))

(addtest (mongo-bson-test)
  encode-basic-test-2
  (ensure-same (encode '(:test "hello world") :list)
               (list #x1B #x00 #x00 #x00 #x02 #x74 #x65 #x73 #x74 #x00 #x0C #x00 #x00
                     #x00 #x68 #x65 #x6C #x6C #x6F #x20 #x77 #x6F #x72 #x6C #x64 #x00
                     #x00)))

(addtest (mongo-bson-test)
  encode-basic-test-3
  (ensure-same (encode '(:mike 100) :list)
               (list #x0F #x00 #x00 #x00 #x10 #x6D #x69 #x6B #x65 #x00 #x64 #x00 #x00
                     #x00 #x00)))

(addtest (mongo-bson-test)
  encode-basic-test-4
  (ensure-same (encode '(:hello 1.5) :list)
               (list #x14 #x00 #x00 #x00 #x01 #x68 #x65 #x6C #x6C #x6F #x00 #x00 #x00
                     #x00 #x00 #x00 #x00 #xF8 #x3F #x00)))

(addtest (mongo-bson-test)
  encode-basic-test-5
  (ensure-same (encode '(:true t) :list)
               (list #x0C #x00 #x00 #x00 #x08 #x74 #x72 #x75 #x65 #x00 #x01 #x00)))

(addtest (mongo-bson-test)
  encode-basic-test-6
  (ensure-same (encode '(:false nil) :list)
               (list #x0D #x00 #x00 #x00 #x08 #x66 #x61 #x6C #x73 #x65 #x00 #x00
                     #x00)))

(addtest (mongo-bson-test)
  encode-basic-test-7
  (ensure-same (encode '(:empty #()) :list)
                (list #x11 #x00 #x00 #x00 #x04 #x65 #x6D #x70 #x74 #x79 #x00 #x05 #x00
                      #x00 #x00 #x00 #x00)))

(addtest (mongo-bson-test)
  encode-basic-test-8
  (ensure-same (encode (list :none (make-hash-table)) :list)
               (list #x10 #x00 #x00 #x00 #x03 #x6E #x6F #x6E #x65 #x00 #x05 #x00 #x00
                     #x00 #x00 #x00)))

(addtest (mongo-bson-test)
  encode-basic-test-9
  (ensure-same (encode
                (list :oid (make-instance 'mongo.bson:object-id
                                          :raw (list #x00 #x01 #x02 #x03 #x04 #x05 #x06 #x07 #x08 #x09 #x0A #x0B)))
                :list)
               (list #x16 #x00 #x00 #x00 #x07 #x6F #x69 #x64 #x00 #x00 #x01 #x02 #x03
                     #x04 #x05 #x06 #x07 #x08 #x09 #x0A #x0B #x00)))

(addtest (mongo-bson-test)
  encode-then-decode-1
  (ensure-same (decode (encode '(:mike -10120) :vector))
               '(:mike -10120)))

(addtest (mongo-bson-test)
  encode-then-decode-2
  (ensure-same (decode (encode '(:really-bing-long 2147483648) :vector))
               '(:really-bing-long 2147483648)))

(addtest (mongo-bson-test)
  encode-then-decode-3
  (ensure-same (abs (- (getf (decode (encode '(:hello 0.0013109) :vector))
                             :hello)
                       0.0013109))
               0.0d0))

(addtest (mongo-bson-test)
  encode-then-decode-4
  (ensure-same (decode (encode '(:something t) :vector))
               '(:something t)))

(addtest (mongo-bson-test)
  encode-then-decode-5
  (ensure-same (decode (encode '(:false nil) :vector))
               '(:false nil)))

(addtest (mongo-bson-test)
  encode-then-decode-6
  (destructuring-bind (s (i b f str))
      (decode (encode '(:array #(1 t 3.8 "world")) :vector))
    (ensure-same s :array)
    (ensure-same i 1)
    (ensure-same b t)
    (ensure (= (- f  3.8) 0.0d0))
    (ensure-same str "world")))

(addtest (mongo-bson-test)
  encode-then-decode-7
  (ensure-same (decode (encode '(:object (:test "something")) :vector))
               '(:object (:test "something"))))

(addtest (mongo-bson-test)
  encode-then-decode-8
  (ensure-same (decode (encode '(:big-float 10000000000.d0) :vector))
               '(:big-float 10000000000.d0)))

               


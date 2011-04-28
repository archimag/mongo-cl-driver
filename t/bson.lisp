;;;; bson.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver.test)

(deftestsuite mongo-bson-test (mogno-cl-driver-test) ())

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; encode-basic-test
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(addtest (mongo-bson-test)
  encode-basic-test-1
  (ensure-same (encode (son) :list)
               '(#x05 #x00 #x00 #x00 #x00)))

(addtest (mongo-bson-test)
  encode-basic-test-2
  (ensure-same (encode (son "test" "hello world") :list)
               (list #x1B #x00 #x00 #x00 #x02 #x74 #x65 #x73 #x74 #x00 #x0C #x00 #x00
                     #x00 #x68 #x65 #x6C #x6C #x6F #x20 #x77 #x6F #x72 #x6C #x64 #x00
                     #x00)))

(addtest (mongo-bson-test)
  encode-basic-test-3
  (ensure-same (encode (son "mike" 100) :list)
               (list #x0F #x00 #x00 #x00 #x10 #x6D #x69 #x6B #x65 #x00 #x64 #x00 #x00
                     #x00 #x00)))

(addtest (mongo-bson-test)
  encode-basic-test-4
  (ensure-same (encode (son "hello" 1.5) :list)
               (list #x14 #x00 #x00 #x00 #x01 #x68 #x65 #x6C #x6C #x6F #x00 #x00 #x00
                     #x00 #x00 #x00 #x00 #xF8 #x3F #x00)))

(addtest (mongo-bson-test)
  encode-basic-test-5
  (ensure-same (encode (son "true" t) :list)
               (list #x0C #x00 #x00 #x00 #x08 #x74 #x72 #x75 #x65 #x00 #x01 #x00)))

(addtest (mongo-bson-test)
  encode-basic-test-6
  (ensure-same (encode (son "false" nil) :list)
               (list #x0D #x00 #x00 #x00 #x08 #x66 #x61 #x6C #x73 #x65 #x00 #x00
                     #x00)))

(addtest (mongo-bson-test)
  encode-basic-test-7
  (ensure-same (encode (son "empty" #()) :list)
                (list #x11 #x00 #x00 #x00 #x04 #x65 #x6D #x70 #x74 #x79 #x00 #x05 #x00
                      #x00 #x00 #x00 #x00)))

(addtest (mongo-bson-test)
  encode-basic-test-8
  (ensure-same (encode (son "none" (make-hash-table)) :list)
               (list #x10 #x00 #x00 #x00 #x03 #x6E #x6F #x6E #x65 #x00 #x05 #x00 #x00
                     #x00 #x00 #x00)))

(addtest (mongo-bson-test)
  encode-basic-test-9
  (ensure-same (encode (son "test" (make-instance 'binary-data
                                                  :octets (babel:string-to-octets "test" :encoding :utf-8)))
                       :list)
               (list #x14 #x00 #x00 #x00 #x05 #x74 #x65 #x73 #x74 #x00 #x04 #x00 #x00
                     #x00 #x00 #x74 #x65 #x73 #x74 #x00)))

(addtest (mongo-bson-test)
  encode-basic-test-10
  (ensure-same (encode (son "test" (make-instance 'binary-data
                                                  :octets (babel:string-to-octets "test" :encoding :utf-8)
                                                  :subtype :user-defined))
                       :list)
               (list #x14 #x00 #x00 #x00 #x05 #x74 #x65 #x73 #x74 #x00 #x04 #x00 #x00
                     #x00 #x80 #x74 #x65 #x73 #x74 #x00)))

(addtest (mongo-bson-test)
  encode-basic-test-11
  (ensure-same (encode (son "regex" (make-instance 'regex
                                                   :pattern "a*b"
                                                   :options "i"))
                       :list)
               (list #x12 #x00 #x00 #x00 #x0B #x72 #x65 #x67 #x65 #x78 #x00 #x61 #x2A
                     #x62 #x00 #x69 #x00 #x00)))

(addtest (mongo-bson-test)
  encode-basic-test-12
  (ensure-same (encode (son "$where" (make-instance 'javascript :code "test"))
                       :list)
               (list #x1F #x00 #x00 #x00 #x0F #x24 #x77 #x68 #x65 #x72 #x65 #x00 #x12
                     #x00 #x00 #x00 #x05 #x00 #x00 #x00 #x74 #x65 #x73 #x74 #x00 #x05
                     #x00 #x00 #x00 #x00 #x00)))

(addtest (mongo-bson-test)
  encode-basic-test-13
  (ensure-same (encode
                (son "oid" (make-instance 'mongo.bson:object-id
                                          :raw (list #x00 #x01 #x02 #x03 #x04 #x05 #x06 #x07 #x08 #x09 #x0A #x0B)))
                :list)
               (list #x16 #x00 #x00 #x00 #x07 #x6F #x69 #x64 #x00 #x00 #x01 #x02 #x03
                     #x04 #x05 #x06 #x07 #x08 #x09 #x0A #x0B #x00)))

(addtest (mongo-bson-test)
  encode-basic-test-14
  (ensure-same (encode
                (son "date" (local-time:encode-timestamp 0 11 30 0 8 1 2007 :timezone local-time:+utc-zone+))
                :list)
               (list #x13 #x00 #x00 #x00 #x09 #x64 #x61 #x74 #x65 #x00 #x38 #xBE #x1C
                     #xFF #x0F #x01 #x00 #x00 #x00)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; encode-then-decode
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(addtest (mongo-bson-test)
  encode-then-decode-1
  (ensure-same (hash-table-alist (decode (encode (son "miki" -10120) :vector)))
               '(("miki" . -10120))))

(addtest (mongo-bson-test)
  encode-then-decode-2
  (ensure-same (hash-table-alist (decode (encode (son "reallyBingLong" 2147483648) :vector)))
               '(("reallyBingLong" . 2147483648))))

(addtest (mongo-bson-test)
  encode-then-decode-3
  (destructuring-bind ((str . float)) (hash-table-alist (decode (encode (son "hello" 0.0013109) :vector)))
    (ensure-same str "hello")
    (ensure-same (abs (- float 0.0013109))
                 0.0d0)))

(addtest (mongo-bson-test)
  encode-then-decode-4
  (ensure-same (hash-table-alist (decode (encode (son "something" t) :vector)))
               '(("something" . t))))

(addtest (mongo-bson-test)
  encode-then-decode-5
  (ensure-same (hash-table-alist (decode (encode (son "false" nil) :vector)))
               '(("false" . nil))))

(addtest (mongo-bson-test)
  encode-then-decode-6
  (destructuring-bind ((s i b f str))
      (hash-table-alist (decode (encode (son "array" #(1 t 3.8 "world")) :vector)))
    (ensure-same s "array")
    (ensure-same i 1)
    (ensure-same b t)
    (ensure (= (- f  3.8) 0.0d0))
    (ensure-same str "world")))

(addtest (mongo-bson-test)
  encode-then-decode-7
  (destructuring-bind ((name . obj))
      (hash-table-alist (decode (encode (son "object" (son "test" "something")) :vector)))
    (ensure-same name "object")
    (ensure-same (hash-table-alist obj)
                 '(("test" . "something")))))

(addtest (mongo-bson-test)
  encode-then-decode-8
  (ensure-same (hash-table-alist (decode (encode (son "bigFloat" 10000000000.d0) :vector)))
               '(("bigFloat" . 10000000000.d0))))

               
(addtest (mongo-bson-test)
  encode-the-decode-9
  (destructuring-bind ((name . date))
      (hash-table-alist
       (decode (encode (son "date"
                            (local-time:encode-timestamp 0 0 0 2 4 4 1993
                                                         :timezone local-time:+utc-zone+))
                       :vector)))
    (ensure-same "date" name)
    (ensure (local-time:timestamp= date
                                   (local-time:encode-timestamp 0 0 0 2 4 4 1993 :timezone local-time:+utc-zone+)))))

(addtest (mongo-bson-test)
  encode-the-decode-10
  (destructuring-bind ((name . obj))
      (hash-table-alist
       (decode (encode (son "a binary"
                            (make-instance 'binary-data
                                           :octets (coerce #(1 2 3 4 5 6 7 8 9 10) 'ub8-vector)
                                           :subtype :function))
                       :vector)))
    (ensure-same "a binary" name)
    (ensure-same :function 
                 (binary-data-subtype obj))
    (ensure-same '(1 2 3 4 5 6 7 8 9 10)
                 (coerce (binary-data-octets obj) 'list))))

(addtest (mongo-bson-test)
  encode-the-decode-11
  (destructuring-bind ((name . obj))
      (hash-table-alist
       (decode (encode (son "a binary"
                            (make-instance 'binary-data
                                           :octets (coerce #(1 2 3 4 5 6 7 8 9 10) 'ub8-vector)
                                           :subtype :uuid))
                       :vector)))
    (ensure-same "a binary" name)
    (ensure-same :uuid
                 (binary-data-subtype obj))
    (ensure-same '(1 2 3 4 5 6 7 8 9 10)
                 (coerce (binary-data-octets obj) 'list))))

(addtest (mongo-bson-test)
  encode-the-decode-12
  (destructuring-bind ((name . obj))
      (hash-table-alist
       (decode (encode (son "a binary"
                            (make-instance 'binary-data
                                           :octets (coerce #(1 2 3 4 5 6 7 8 9 10) 'ub8-vector)
                                           :subtype :md5))
                       :vector)))
    (ensure-same "a binary" name)
    (ensure-same :md5
                 (binary-data-subtype obj))
    (ensure-same '(1 2 3 4 5 6 7 8 9 10)
                 (coerce (binary-data-octets obj) 'list))))

(addtest (mongo-bson-test)
  encode-the-decode-13
  (ensure-same '(("foo" . +min-key+))
               (hash-table-alist (decode (encode (son "foo" +min-key+) :vector)))))

(addtest (mongo-bson-test)
  encode-the-decode-14
  (ensure-same '(("foo" . +max-key+))
               (hash-table-alist (decode (encode (son "foo" +max-key+) :vector)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; errors
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(addtest (mongo-bson-test)
  type-error
  (ensure-condition type-error
    (encode 100 :list))
  (ensure-condition type-error
    (encode "hello" :vector))
  (ensure-condition type-error
    (encode nil :vector))
  (ensure-condition type-error
    (encode #() :vector))

  (ensure (encode (son "x" 9223372036854775807) :vector))
  (ensure-condition type-error
    (encode (son "x" 9223372036854775808) :vector))

  (ensure (encode (son "x" -9223372036854775808) :vector))
  (ensure-condition type-error
    (encode (son "x" -9223372036854775809) :vector))

  (ensure-condition type-error
    (encode (son 8.9 "test") :vector)))



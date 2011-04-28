;;;; package.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(defpackage #:mongo-cl-driver.bson
  (:nicknames #:mongo.bson)
  (:use #:cl #:iter)
  (:export #:encode
           #:decode

           #:ub8
           #:ub8-sarray
           #:ub8-vector
           
           #:object-id
           #:+min-key+
           #:+max-key+
           #:binary-data
           #:binary-data-subtype
           #:binary-data-octets
           #:regex
           #:regex-pattern
           #:regex-options
           #:javascript
           #:javascript-code
           #:javascript-scope
           #:mongo-timestamp
           #:mongo-timestamp-value

           #:encode-byte
           #:decode-byte
           #:encode-int32
           #:decode-int32
           #:encode-int64
           #:decode-int64
           #:encode-cstring
           #:decode-cstring
           #:encode-document
           #:decode-document

           #:with-count-encoded-bytes
           #:*encoded-bytes-count*
           #:*decoded-bytes-count*
           #:bson-target-replace))

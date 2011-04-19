;;;; packages.lisp
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
           
           #:object-id

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

           #:bson-target-replace

           #:*lisp-identifier-name-to-bson*
           #:*bson-identifier-name-to-lisp*))

(defpackage #:mongo-cl-driver.wire
  (:nicknames #:mongo.wire)
  (:use #:iter #:mongo-cl-driver.bson #:closer-common-lisp)
  (:export #:brigade
           #:brigade-extend
           #:brigade-free-buckets
           #:brigade-ref

           #:connection
           #:send-message
           #:read-reply

           #:op-update
           #:op-insert
           #:op-query
           #:op-getmore
           #:op-delete

           #:op-reply
           #:op-reply-response-flags
           #:op-reply-cursor-id
           #:op-reply-starting-from
           #:op-reply-number-returned
           #:op-reply-documents))
           
(defpackage #:mongo-cl-driver
  (:nicknames #:mongo)
  (:use #:cl #:iter #:mongo-cl-driver.wire))
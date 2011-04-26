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
           #:*bson-identifier-name-to-lisp*

           #:*convert-bson-document-to-lisp*
           #:decode-document-to-alist
           #:decode-document-to-plist
           #:decode-document-to-hashtable))

(defpackage #:mongo-cl-driver.wire
  (:nicknames #:mongo.wire)
  (:use #:iter #:mongo-cl-driver.bson #:closer-common-lisp)
  (:export #:brigade
           #:brigade-extend
           #:brigade-free-buckets
           #:brigade-ref

           #:connection
           #:close-connection
           #:send-message-sync
           #:send-message-async
           #:send-and-read-sync
           #:send-and-read-async

           #:op-update
           #:op-insert
           #:op-query
           #:op-getmore
           #:op-delete
           #:op-kill-cursors

           #:op-reply
           #:op-reply-response-flags
           #:op-reply-cursor-id
           #:op-reply-starting-from
           #:op-reply-number-returned
           #:op-reply-documents))
           
(defpackage #:mongo-cl-driver
  (:nicknames #:mongo)
  (:use #:cl
        #:iter #:alexandria
        #:mongo-cl-driver.wire)
  (:import-from #:mongo-cl-driver.bson #:object-id)
  (:export #:connection

           #:object-id

           #:database
           #:close-database
           #:with-database
           #:run-command
           #:last-error
           #:db-stats
           #:cursor-info
           #:authenticate
           #:logout

           #:collection
           #:collection-names
           #:create-collection
           #:drop-collection
           #:validate-collection
           #:collection-count

           #:find-one
           #:find-one-async
           #:find-cursor
           #:find-list
           #:find-cursor-async
           #:insert-op
           #:update-op
           #:delete-op

           #:cursor
           #:with-cursor
           #:with-cursor-async
           #:docursor
           #:close-cursor))

(defpackage #:mongo-cl-driver.son-sugar
  (:nicknames #:son-sugar)
  (:use #:cl #:iter)
  (:export #:son
           #:use-son-printer
           #:with-son-printer
           #:print-son))
  
(defpackage #:mongo-cl-driver-user
  (:nicknames #:mongo-user)
  (:use #:cl #:iter
        #:mongo-cl-driver #:mongo-cl-driver.son-sugar))
;;;; package.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

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
           #:encode-protocol-message
           #:decode-op-reply

           #:op-reply
           #:op-reply-response-flags
           #:op-reply-cursor-id
           #:op-reply-starting-from
           #:op-reply-number-returned
           #:op-reply-documents
           #:cursor-not-found-p
           #:query-failure-p
           #:await-capable-p))
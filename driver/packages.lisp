;;;; packages.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(defpackage #:mongo-cl-driver
  (:nicknames #:mongo )
  (:use #:cl
        #:iter #:alexandria
        #:blackbird
        #:mongo-cl-driver.wire
        #:mongo-cl-driver.adapters
        #:mongo-cl-driver.sugar)
  (:import-from #:mongo-cl-driver.adapters #:mongo-client-close)
  (:shadow #:defconstant #:find #:remove)
  (:shadowing-import-from #:iterate #:finally)
  (:export #:mongo-client
           #:create-mongo-client
           #:mongo-client-close
           #:with-client

           ;; conditions
           #:mongo-condition
           #:mongo-error
           #:connection-failure
           #:operation-failure
           ;;#:timeout-failure
           #:duplicate-key-error
           ;;#:invalid-operation

           ;; write concern
           #:write-concern
           #:write-concern-w
           #:write-concern-j
           #:write-concern-fsync
           #:write-concern-wtimeout
           #:+write-concern-normal+
           #:+write-concern-fsync+
           #:+write-concern-replicas+
           #:+write-concern-fast+
           #:+write-concern-crazy+

           ;; server config
           #:server-config
           #:server-hostname
           #:server-port

           ;; database
           #:database
           #:authenticate
           #:logout
           #:run-command
           #:last-error
           #:previous-error
           #:reset-error
           #:collection-names
           #:create-collection
           #:drop-collection
           #:eval-js
           #:stats
           #:cursor-info

           ;; collection
           #:collection
           #:find-one
           #:find
           #:insert
           #:update
           #:save
           #:remove
           #:$count
           #:$distinct
           
           ;; cursor
           #:*cursor-batch-size*
           #:cursor
           #:close-cursor
           #:find-cursor
           #:iterate-cursor
           #:docursor
           #:with-cursor
           #:with-cursor-sync
           ))

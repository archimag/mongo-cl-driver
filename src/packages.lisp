;;;; packages.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(defpackage #:mongo-cl-driver.son-sugar
  (:nicknames #:son-sugar)
  (:use #:cl #:iter)
  (:export #:son
           #:use-son-printer
           #:with-son-printer
           #:print-son))

(defpackage #:mongo-cl-driver
  (:nicknames #:mongo)
  (:use #:cl
        #:iter #:alexandria
        #:mongo-cl-driver.wire #:mongo-cl-driver.son-sugar)
  (:import-from #:mongo-cl-driver.bson #:object-id)
  (:export #:connection

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
           #:find-list-async
           #:find-cursor-async
           #:insert-op
           #:update-op
           #:delete-op

           #:cursor
           #:with-cursor
           #:with-cursor-async
           #:docursor
           #:close-cursor))

(defpackage #:mongo-cl-driver-user
  (:nicknames #:mongo-user)
  (:use #:cl #:iter
        #:mongo-cl-driver #:mongo-cl-driver.son-sugar))
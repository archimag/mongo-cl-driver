;;;; wire.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver.test)

(deftestsuite mongo-wire-test (mogno-cl-driver-test) ())

(deftype ub8 () '(unsigned-byte 8))

(defun make-vector-target ()
  (make-array 0 :element-type 'ub8 :fill-pointer 0 :adjustable 0))

(defun encode-int32-list (int32)
  (let ((target (make-vector-target)))
    (encode-int32 int32 target)
    (coerce target 'list)))

(defun add-length-and-convert-to-list (target)
  (concatenate 'list
               (encode-int32-list (+ (length target) 4))
               (coerce target 'list)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; encode op-update
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(addtest (mongo-wire-test)
  encode-op-update-1
  (let ((target (make-vector-target))
        (*encoded-bytes-count* 0))
    (encode-int32 0 target)              ;; requestId
    (encode-int32 0 target)              ;; responseTo
    (encode-int32 2001 target)           ;; opcode
    (encode-int32 0 target)              ;; zero
    (encode-cstring "db.test" target)    ;; fullConnectionName
    (encode-int32 0 target)              ;; flags
    (encode-document (son "x" 2) target) ;; selector
    (encode-document (son "x" 3) target) ;; update

    (ensure-same (add-length-and-convert-to-list target)
                 (encode-protocol-message (make-instance 'op-update
                                                         :full-collection-name "db.test"
                                                         :selector (son "x" 2)
                                                         :update (son "x" 3))
                                          :list))))

(addtest (mongo-wire-test)
  encode-op-update-2
  (let ((target (make-vector-target))
        (*encoded-bytes-count* 0))
    (encode-int32 0 target)                          ;; requestId
    (encode-int32 0 target)                          ;; responseTo
    (encode-int32 2001 target)                       ;; opcode
    (encode-int32 0 target)                          ;; zero
    (encode-cstring "database.$cmd" target)          ;; fullConnectionName
    (encode-int32 2 target)                          ;; flags
    (encode-document (son "x" "Hello world") target) ;; selector
    (encode-document (son "x" #(3 4 5)) target)      ;; update

    (ensure-same (add-length-and-convert-to-list target)
                 (encode-protocol-message (make-instance 'op-update
                                                         :full-collection-name "database.$cmd"
                                                         :upsert nil
                                                         :multi-update t
                                                         :selector (son "x" "Hello world")
                                                         :update (son "x" #(3 4 5)))
                                          :list))))

(addtest (mongo-wire-test)
  encode-op-update-3
  (let ((target (make-vector-target))
        (*encoded-bytes-count* 0))
    (encode-int32 1 target)                        ;; requestId
    (encode-int32 10 target)                       ;; responseTo
    (encode-int32 2001 target)                     ;; opcode
    (encode-int32 0 target)                        ;; zero
    (encode-cstring "db.system.index" target)      ;; fullConnectionName
    (encode-int32 3 target)                        ;; flags
    (encode-document (son "x" (son "y" 2)) target) ;; selector
    (encode-document (son "x" #(3 4 5)) target)    ;; update

    (ensure-same (add-length-and-convert-to-list target)
                 (encode-protocol-message (make-instance 'op-update
                                                         :request-id 1
                                                         :response-to 10
                                                         :full-collection-name "db.system.index"
                                                         :upsert t
                                                         :multi-update t
                                                         :selector (son "x" (son "y" 2))
                                                         :update (son "x" #(3 4 5)))
                                          :list))))
        
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; encode op-insert
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(addtest (mongo-wire-test)
  encode-op-insert-1
  (let ((target (make-vector-target))
        (*encoded-bytes-count* 0))
    (encode-int32 0 target)           ;; requestId
    (encode-int32 0 target)           ;; responseTo
    (encode-int32 2002 target)        ;; opcode
    (encode-int32 0 target)           ;; zero
    (encode-cstring "db.test" target) ;; fullConnectionName
    ;; documents
    (encode-document (son "x" 2) target) 

    (ensure-same (add-length-and-convert-to-list target)
                 (encode-protocol-message (make-instance 'op-insert
                                                         :full-collection-name "db.test"
                                                         :documents (list (son "x" 2)))
                                          :list))))

(addtest (mongo-wire-test)
  encode-op-insert-2
  (let ((target (make-vector-target))
        (*encoded-bytes-count* 0))
    (encode-int32 8 target)           ;; requestId
    (encode-int32 0 target)           ;; responseTo
    (encode-int32 2002 target)        ;; opcode
    (encode-int32 0 target)           ;; zero
    (encode-cstring "db.test" target) ;; fullConnectionName
    ;; documents
    (encode-document (son "x" 2) target)
    (encode-document (son "date" (local-time:encode-timestamp 0 0 10 23 27 4 2011)) target)
    (encode-document (son "foo" (son "bar" #(1 2 3 4))) target)

    (ensure-same (add-length-and-convert-to-list target)
                 (encode-protocol-message
                  (make-instance 'op-insert
                                 :request-id 8
                                 :full-collection-name "db.test"
                                 :documents (list (son "x" 2)
                                                  (son "date" (local-time:encode-timestamp 0 0 10 23 27 4 2011))
                                                  (son "foo" (son "bar" #(1 2 3 4)))))
                  :list))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; encode op-query
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(addtest (mongo-wire-test)
  encode-op-query-1
  (let ((target (make-vector-target))
        (*encoded-bytes-count* 0))
    (encode-int32 0 target)              ;; requestId
    (encode-int32 0 target)              ;; responseTo
    (encode-int32 2004 target)           ;; opcode
    (encode-int32 162 target)            ;; flags
    (encode-cstring "db.test" target)    ;; fullConnectionName
    (encode-int32 0 target)              ;; numberToSkip
    (encode-int32 0 target)              ;; numberToReturn
    (encode-document (son "x" 2) target) ;; query
    (encode-document (son) target)       ;; selector

    (ensure-same (add-length-and-convert-to-list target)
                 (encode-protocol-message (make-instance 'op-query
                                                         :full-collection-name "db.test"
                                                         :query (son "x" 2)
                                                         :tailable-cursor t
                                                         :await-data t
                                                         :partial t)
                                          :list))))

(addtest (mongo-wire-test)
  encode-op-query-2
  (let ((target (make-vector-target))
        (*encoded-bytes-count* 0))
    (encode-int32 0 target)              ;; requestId
    (encode-int32 0 target)              ;; responseTo
    (encode-int32 2004 target)           ;; opcode
    (encode-int32 84 target)             ;; flags
    (encode-cstring "db.test" target)    ;; fullConnectionName
    (encode-int32 0 target)              ;; numberToSkip
    (encode-int32 0 target)              ;; numberToReturn
    (encode-document (son "x" 2) target) ;; query
    (encode-document (son "title" 1)     ;; returnFieldSelector
                     target)       

    (ensure-same (add-length-and-convert-to-list target)
                 (encode-protocol-message (make-instance 'op-query
                                                         :full-collection-name "db.test"
                                                         :query (son "x" 2)
                                                         :return-field-selector (son "title" 1)
                                                         :slave-ok t
                                                         :no-cursor-timeout t
                                                         :exhaust t)
                                          :list))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; encode op-getmore
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(addtest (mongo-wire-test)
  encode-op-getmore-1
  (let ((target (make-vector-target))
        (*encoded-bytes-count* 0))
    (encode-int32 0 target)           ;; requestId
    (encode-int32 0 target)           ;; responseTo
    (encode-int32 2005 target)        ;; opcode
    (encode-int32 0 target)           ;; zero
    (encode-cstring "db.test" target) ;; fullConnectionName
    (encode-int32 20 target)          ;; numberToReturn
    (encode-int64 123 target)         ;; cursorID

    (ensure-same (add-length-and-convert-to-list target)
                 (encode-protocol-message (make-instance 'op-getmore
                                                         :full-collection-name "db.test"
                                                         :number-to-return 20
                                                         :cursor-id 123)
                                          :list))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; encode op-delete
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(addtest (mongo-wire-test)
  encode-op-delete-1
  (let ((target (make-vector-target))
        (*encoded-bytes-count* 0))
    (encode-int32 0 target)            ;; requestId
    (encode-int32 0 target)            ;; responseTo
    (encode-int32 2006 target)         ;; opcode
    (encode-int32 0 target)            ;; zero
    (encode-cstring "db.test" target)  ;; fullConnectionName
    (encode-int32 0 target)            ;; flags
    (encode-document (son "foo" "bar") ;; selector
                     target)

    (ensure-same (add-length-and-convert-to-list target)
                 (encode-protocol-message (make-instance 'op-delete
                                                         :full-collection-name "db.test"
                                                         :selector (son "foo" "bar"))
                                          :list))))

(addtest (mongo-wire-test)
  encode-op-delete-2
  (let ((target (make-vector-target))
        (*encoded-bytes-count* 0))
    (encode-int32 0 target)            ;; requestId
    (encode-int32 0 target)            ;; responseTo
    (encode-int32 2006 target)         ;; opcode
    (encode-int32 0 target)            ;; zero
    (encode-cstring "db.test" target)  ;; fullConnectionName
    (encode-int32 1 target)            ;; flags
    (encode-document (son "foo" "bar") ;; selector
                     target)

    (ensure-same (add-length-and-convert-to-list target)
                 (encode-protocol-message (make-instance 'op-delete
                                                         :full-collection-name "db.test"
                                                         :single-remove t
                                                         :selector (son "foo" "bar"))
                                          :list))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; encode op-kill-cursor
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(addtest (mongo-wire-test)
  encode-op-kill-cursor-1
  (let ((target (make-vector-target))
        (*encoded-bytes-count* 0))
    (encode-int32 0 target)    ;; requestId
    (encode-int32 0 target)    ;; responseTo
    (encode-int32 2007 target) ;; opcode
    (encode-int32 0 target)    ;; zero
    (encode-int32 1 target)    ;; numberOfCursorIDs
    ;; sequence of cursorIDs to close
    (encode-int64 123 target) 

    (ensure-same (add-length-and-convert-to-list target)
                 (encode-protocol-message (make-instance 'op-kill-cursors
                                                         :cursor-ids '(123))
                                          :list))))

(addtest (mongo-wire-test)
  encode-op-kill-cursor-1
  (let ((target (make-vector-target))
        (*encoded-bytes-count* 0))
    (encode-int32 0 target)    ;; requestId
    (encode-int32 0 target)    ;; responseTo
    (encode-int32 2007 target) ;; opcode
    (encode-int32 0 target)    ;; zero
    (encode-int32 3 target)    ;; numberOfCursorIDs
    ;; sequence of cursorIDs to close
    (encode-int64 123 target) 
    (encode-int64 256 target)
    (encode-int64 2561231 target) 

    (ensure-same (add-length-and-convert-to-list target)
                 (encode-protocol-message (make-instance 'op-kill-cursors
                                                         :cursor-ids '(123 256 2561231))
                                          :list))))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; decode op-reply
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(addtest (mongo-wire-test)
  decode-op-reply-1
  (let ((target (make-vector-target))
        (*encoded-bytes-count* 0)
        (*decoded-bytes-count* 0))
    (encode-int32 0 target)   ;; requestId
    (encode-int32 0 target)   ;; responseTo
    (encode-int32 1 target)   ;; opcode
    (encode-int32 0 target)   ;; responseFlags
    (encode-int64 456 target) ;; cursorID
    (encode-int32 7 target)   ;; startingFrom
    (encode-int32  1 target)  ;; numberReturned
    ;; documents
    (encode-document (son "foo" "bar") target)

    (let ((reply (decode-op-reply (coerce (add-length-and-convert-to-list target) 'vector))))
      (ensure-same 456 (op-reply-cursor-id reply))
      (ensure-same 7 (op-reply-starting-from reply))
      (ensure-same 1 (op-reply-number-returned reply))
      (ensure-same "bar" (gethash "foo" (car (op-reply-documents reply)))))))


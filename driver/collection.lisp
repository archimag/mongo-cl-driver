;;;; collection.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver)

(defclass collection ()
  ((database :initarg :database :reader collection-database)
   (name :initarg :name :reader collection-name)
   (indexes :initform nil :accessor collection-indexes)
   (fullname :reader fullname)))

(defun collection (database name)
  (make-instance 'collection
                 :database database
                 :name name))

(defmethod mongo-client ((collection collection))
  (mongo-client (collection-database collection)))

(defmethod shared-initialize :after ((collection collection) slot-names &key)
  (let ((name (collection-name collection)))
    (check-collection-name name)
    (setf (slot-value collection 'fullname)
          (format nil "~A.~A" (database-name (collection-database collection)) name))))

(defmethod print-object ((col collection) stream)
  (print-unreadable-object (col stream :type t :identity t)
    (princ (fullname col) stream)))

;;;; CRUD

(defun find-one (collection &key query selector)
  (try-unpromisify
   (alet ((reply (send-message-and-read-reply
                  (mongo-client collection)
                  (make-instance 'op-query
                                 :number-to-return 1
                                 :full-collection-name (fullname collection)
                                 :query query
                                 :return-field-selector selector))))
     (first (op-reply-documents reply)))))

(defun find (collection &key query fields (limit 0) (skip 0))
  (try-unpromisify
   (chain (send-message-and-read-reply (mongo-client collection)
                                       (make-instance 'op-query
                                                      :full-collection-name (fullname collection)
                                                      :number-to-return limit
                                                      :number-to-skip skip
                                                      :query query
                                                      :return-field-selector fields))
     #|-----------------------------------------------------------------------|#
     (:attach (reply)
       (wait (send-message (mongo-client collection)
                           (make-instance 'op-kill-cursors
                                          :cursor-ids (list (op-reply-cursor-id reply))))
         (op-reply-documents reply))))))

(defun %send-message (db message &key write-concern)
  (unless write-concern
    (setf write-concern (write-concern db)))
  #|--------------------------------------------------------------------------|#
  (if (member (write-concern-w write-concern) '(:errors-ignored :unacknowledged))
      (send-message (mongo-client db) message :write-concern write-concern)
      (wait (send-message (mongo-client db) message :write-concern write-concern)
        (wait (run-command db ($ "getLastError" (write-concern-options write-concern)))))))

(defun insert (collection object &key write-concern)
  (try-unpromisify
   (%send-message (collection-database collection)
                  (make-instance 'op-insert
                                 :full-collection-name (fullname collection)
                                 :documents (if (listp object)
                                                object
                                                (list object)))
                  :write-concern write-concern)))

(defun update (collection selector update &key upsert multi-update write-concern)
  (try-unpromisify
   (%send-message (collection-database collection)
                  (make-instance 'op-update
                                 :full-collection-name (fullname collection)
                                 :selector selector
                                 :update update
                                 :upsert upsert
                                 :multi-update multi-update)
                  :write-concern write-concern)))

(defun save (collection object &key write-concern)
  (if (gethash "_id" object)
      (update collection ($ "_id" (gethash "_id" object)) object :write-concern write-concern)
      (insert collection object :write-concern write-concern)))

(defun remove (collection selector &key single-remove write-concern)
  (try-unpromisify
   (%send-message (collection-database collection)
                  (make-instance 'op-delete
                                 :full-collection-name (fullname collection)
                                 :selector selector
                                 :single-remove single-remove)
                  :write-concern write-concern)))

;;;; Aggregation

(defun $count (collection &optional query)
  (let ((cmd ($ "count" (collection-name collection))))
    (when query
      (setf (gethash "query" cmd) query))
    (try-unpromisify
     (alet ((reply (run-command (collection-database collection) cmd)))
       (gethash "n" reply)))))

(defun $distinct (collection field &key query)
  (let ((cmd ($ "distinct" (collection-name collection)
                "key" field)))
    (when query
      (setf (gethash "query" cmd) query))
    (try-unpromisify
     (alet ((distinct (run-command (collection-database collection) cmd)))
       (gethash "values" distinct)))))

(defun aggregate (collection &rest pipeline)
  (let ((cmd ($ "aggregate" (collection-name collection)
                "pipeline" pipeline)))
    (try-unpromisify
     (alet ((reply (run-command (collection-database collection) cmd)))
           (gethash "result" reply)))))


;;;; TODO

;; options

;; findAndModify

;; mapReduce
;; group

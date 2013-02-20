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
   (fullname :reader fullname)))

(defmethod connection ((collection collection))
  (connection (collection-database collection)))

(defmethod shared-initialize :after ((collection collection) slot-names &key create options &allow-other-keys)
  (let ((name (collection-name collection))
        (db   (collection-database collection)))
    (check-type name string)
    
    (when (or (string= name "")
              (search ".." name))
      (error "collection names cannot be empty"))

    (when (and (find #\$ name)
               (not (or (starts-with-subseq "oplog.$main" name)
                        (starts-with-subseq "$main" name))))
      (error "collection names must not contain '$': ~A" name))

    (when (or (char= (char name 0) #\.)
              (char= (char name (1- (length name))) #\.))
      (error "collection names must not start or end with '.': ~A" name))

    (when (find (code-char 0) name)
      (error "collection names must not contain the null character"))

    (setf (slot-value collection 'fullname)
          (format nil
                  "~A.~A"
                  (database-name (collection-database collection))
                  name))

    (when (or create options)
      (run-command db
                   `(("create" . ,name) ,@options)))))

(defmethod print-object ((col collection) stream)
  (print-unreadable-object (col stream :type t :identity t)
    (princ (fullname col) stream)))

(defun find-one (collection &optional query selector)
  (first (op-reply-documents
          (send-and-read-sync (connection collection)
                              (make-instance 'op-query
                                             :number-to-return 1
                                             :full-collection-name (fullname collection)
                                             :query query
                                             :return-field-selector selector)))))

(defun find-one-async (collection query callback)
  (send-and-read-async (connection collection)
                       (make-instance 'op-query
                                      :number-to-return 1
                                      :full-collection-name (fullname collection)
                                      :query query)
                       (named-lambda find-one-async-lambda (err reply)
                           (funcall callback
                                    err
                                    (if (not err)
                                        (first (op-reply-documents reply)))))))
                       
  

(defun find-cursor (collection &optional query fields)
  (let ((reply (send-and-read-sync (connection collection)
                                   (make-instance 'op-query
                                                  :full-collection-name (fullname collection)
                                                  :query query
                                                  :return-field-selector fields))))
    (make-instance 'cursor
                   :id (op-reply-cursor-id reply)
                   :collection collection
                   :documents (op-reply-documents reply))))

(defun find-list (collection &key query fields (limit 0) (skip 0))
  (let ((reply (send-and-read-sync (connection collection)
                                   (make-instance 'op-query
                                                  :full-collection-name (fullname collection)
                                                  :number-to-return limit
                                                  :number-to-skip skip
                                                  :query query
                                                  :return-field-selector fields))))
    (unwind-protect
         (progn
           (check-reply reply)
           (op-reply-documents reply))
      (send-message-sync (connection collection)
                         (make-instance 'op-kill-cursors
                                        :cursor-ids (list (op-reply-cursor-id reply)))))))

(defun find-list-async (collection callback &key query fields (limit 0) (skip 0))
  (send-and-read-async (connection collection)
                       (make-instance 'op-query
                                      :full-collection-name (fullname collection)
                                      :number-to-return limit
                                      :number-to-skip skip
                                      :query query
                                      :return-field-selector fields)
                       (named-lambda find-list-async (err reply)
                         (cond
                           (err (funcall callback err nil))
                           (t (handler-case
                                  (check-reply reply)
                                (error (condition) (funcall callback condition nil)))
                              (funcall callback nil (op-reply-documents reply)))))))
                           

(defun find-cursor-async (collection query fields callback)
  (send-and-read-async (connection collection)
                       (make-instance 'op-query
                                      :full-collection-name (fullname collection)
                                      :query query
                                      :return-field-selector fields)
                       (named-lambda find-cursor-async-lambda (err reply)
                           (funcall callback
                                    err
                                    (make-instance 'cursor
                                                   :id (op-reply-cursor-id reply)
                                                   :collection collection
                                                   :documents (op-reply-documents reply))))))

(defun insert-op (collection &rest objects)
  (when objects
    (send-message-sync (connection collection)
                       (make-instance 'op-insert
                                      :full-collection-name (fullname collection)
                                      :documents objects))
    (check-last-error (collection-database collection))))

(defun update-op (collection selector update &key upsert multi-update)
  (send-message-sync (connection collection)
                     (make-instance 'op-update
                                    :full-collection-name (fullname collection)
                                    :selector selector
                                    :update update
                                    :upsert upsert
                                    :multi-update multi-update))
  (check-last-error (collection-database collection)))

(defun delete-op (collection selector &key single-remove)
  (send-message-sync (connection collection)
                     (make-instance 'op-delete
                                    :full-collection-name (fullname collection)
                                    :selector selector
                                    :single-remove single-remove))
  (check-last-error (collection-database collection)))

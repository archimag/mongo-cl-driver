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
   (fullname :reader collection-fullname)))

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
    (princ (collection-fullname col) stream)))

(defun find-one (collection &optional query)
  (let ((conn (connection collection)))
    (send-message conn
                  (make-instance 'op-query
                                 :number-to-return 1
                                 :full-collection-name (collection-fullname collection)
                                 :return-field-selector query))
    (first (op-reply-documents (read-reply conn)))))

(defun find-cursor (collection &optional query)
  (let ((conn (connection collection)))
    (send-message conn
                  (make-instance 'op-query
                                 :full-collection-name (collection-fullname collection)
                                 :return-field-selector query))
    (let ((reply (read-reply conn)))
      (make-instance 'cursor
                     :id (op-reply-cursor-id reply)
                     :collection collection
                     :documents (op-reply-documents reply)))))

(defun insert (collection &rest objects)
  (when objects
    (send-message (connection collection)
                  (make-instance 'op-insert
                                 :full-collection-name (collection-fullname collection)
                                 :documents objects))))
     
  

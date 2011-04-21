;;;; database.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver)

(defmacro with-alist-converter (&body body)
  `(let ((mongo-cl-driver.bson:*convert-bson-document-to-lisp* #'mongo-cl-driver.bson:decode-document-to-alist))
     ,@body))

(defgeneric connection (obj)
  (:documentation "Get connection associated with OBJ"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; database
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defclass database ()
  ((connection :initarg :connection)
   (name :initarg :name :reader database-name)))

(defmethod connection ((db database))
  (slot-value db 'connection))

(defmethod print-object ((db database) stream)
  (print-unreadable-object (db stream :type t :identity t)
    (princ (database-name db) stream)))

(defun run-command (db cmd)
  (if (stringp cmd)
      (run-command db `((,cmd . 1)))
      (let ((conn (connection db)))
        (send-message conn
                      (make-instance 'op-query
                                     :number-to-return 1
                                     :full-collection-name (format nil "~A.$cmd" (database-name db))
                                     :return-field-selector cmd))
        (first (op-reply-documents (read-reply conn))))))

(defun db-stats (db)
  (run-command db "dbStats"))

(defun last-error (db)
  (run-command db "getLastError"))

(defun cursor-info (db)
  (run-command db "cursorInfo"))

(defun collection-names (database)
  "Get a list of all the collection in this DATABASE"
  (let ((conn (connection database))
        (prefix (format nil "~A." (database-name database))))
    (send-message conn
                  (make-instance 'op-query
                                 :full-collection-name (format nil "~A.system.namespaces" (database-name database))
                                 :return-field-selector nil))
    (iter (for item in (op-reply-documents (read-reply conn)))
          (let* ((fullname (gethash "name" item))
                 (name (second (multiple-value-list (starts-with-subseq prefix
                                                                        fullname
                                                                        :return-suffix t)))))
            (when (and name (not (find #\$ name)))
              (collect name))))))


(defun create-collection (database name &key size capped max)
  "Create new Collection in DATABASE.

Normally collection creation is automatic. This method should only be used 
to specify options on creation.

Options:
* :size desired initial size for the collection (in bytes). must be less than
  or equal to 10000000000. For capped collections this size is the max size of
  the collection.
* :capped if T, this is a capped collection 
* :max maximum number of objects if capped "
  
  (when (find name (collection-names database) :test #'string=)
    (error "collection ~S already exists" name))

  (make-instance 'collection
                 :database database
                 :name name
                 :create t
                 :options (remove nil
                                  (plist-alist (list :size size
                                                     :capped capped
                                                     :max max))
                                  :key #'cdr)))

(defun %collection-name (collection)
  (typecase collection
    (string collection)
    (collection (collection-name collection))
    (error "COLLECTION must be an instance of string or collection")))

(defun drop-collection (database collection)
  "Drop a COLLECTION"
  (run-command database
               `(("drop" . ,(%collection-name collection)))))

(defun validate-collection (database collection)
  "Validate a collection.

Returns a string of validation info. An error of type collection-invalid signaled
if validation fails."
  (let* ((name (%collection-name collection))
         (result (run-command database `(("validate" . ,name))))
         (info (gethash "result" result)))
    (when (or (search "exception" info)
              (search "corrupt" info))
      (error "~A invalid: ~A" name info))
    (string-trim #(#\Space #\Newline) info)))

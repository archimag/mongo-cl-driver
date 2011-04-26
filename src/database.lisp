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
  ((connection :initform nil)
   (name :initarg :name :reader database-name)))

(defmethod shared-initialize :after ((db database) slot-names &key hostname port &allow-other-keys)
  (setf (slot-value db 'connection)
        (make-instance 'connection
                       :hostname (or hostname "localhost")
                       :port (or port 27017))))

(defun close-database (database)
  (close-connection (connection database))
  (setf (slot-value database 'connection) nil
        (slot-value database 'name) nil))

(defmacro with-database ((db name &key hostname port) &body body)
  `(let ((,db (make-instance 'database :name ,name :hostname ,hostname :port ,port)))
     (unwind-protect
          (progn ,@body)
       (close-database ,db))))

(defmethod connection ((db database))
  (slot-value db 'connection))

(defmethod print-object ((db database) stream)
  (print-unreadable-object (db stream :type t :identity t)
    (princ (database-name db) stream)))

(defun run-command (db cmd)
  (if (stringp cmd)
      (run-command db `((,cmd . 1)))
      (first (op-reply-documents
              (send-and-read-sync (connection db)
                                  (make-instance 'op-query
                                                 :number-to-return 1
                                                 :full-collection-name (format nil "~A.$cmd" (database-name db))
                                                 :query cmd))))))
             
(defun db-stats (db)
  (run-command db "dbStats"))

(defun last-error (db)
  (run-command db "getLastError"))

(defun cursor-info (db)
  (run-command db "cursorInfo"))

(defun collection-names (database)
  "Get a list of all the collection in this DATABASE"
  (let ((prefix (format nil "~A." (database-name database)))
        (reply (send-and-read-sync (connection database)
                                   (make-instance 'op-query
                                                  :full-collection-name (format nil "~A.system.namespaces" (database-name database))
                                                  :query nil))))
    (iter (for item in (op-reply-documents reply))
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

(defun collection-count (collection &optional query)
  (let ((qr (make-hash-table :test 'equal)))
    (setf (gethash "count" qr)
          (collection-name collection))
    (when query
      (setf (gethash "query" qr) query))
    (gethash "n"
             (run-command (collection-database collection)
                 qr))))
               

(defun collection (database name)
  (make-instance 'collection
                 :database database
                 :name name))

;;;; auth

(defun authenticate (database username password)
  (flet ((md5 (text)
           (ironclad:byte-array-to-hex-string
            (ironclad:digest-sequence
             :md5 (babel:string-to-octets text
                                          :encoding :utf-8)))))
    (let ((son (make-hash-table :test 'equal))
          (nonce (gethash "nonce" (run-command database "getnonce"))))
      (setf (gethash "authenticate" son) 1
            (gethash "user" son) username
            (gethash "nonce" son) nonce)
      
      (setf (gethash "key" son)
            (md5 (format nil
                         "~A~A~A"
                         nonce
                         username
                         (md5 (format nil "~A:mongo:~A" username password)))))
      
      (run-command database son))))

(defun logout (database)
  (run-command database "logout"))
                   

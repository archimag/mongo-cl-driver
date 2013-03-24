;;;; database.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver)

(defgeneric mongo-client (obj)
  (:documentation "Get MongoClient associated with OBJ"))

(defclass database ()
  ((mongo-client :initform nil :initarg :mongo-client :reader mongo-client)
   (name :initarg :name :reader database-name)))

(defmethod write-concern ((db database))
  (write-concern (mongo-client db)))

(defmethod print-object ((db database) stream)
  (print-unreadable-object (db stream :type t :identity t)
    (princ (database-name db) stream)))

;;;; commands

(defun run-command (db cmd)
  (maybe-finished
   (alet ((reply (send-message-and-read-reply
                  (mongo-client db)
                  (make-instance 'op-query
                                 :number-to-return 1
                                 :full-collection-name (format nil "~A.$cmd" (database-name db))
                                 :query (if (stringp cmd) (son cmd t) cmd)))))
     (first (op-reply-documents reply)))))

;;;; errors

(defun last-error (db)
  (run-command db "getLastError"))

(defun previous-error (db)
  (run-command db "getPrevError"))

(defun reset-error (db)
  (run-command db "resetError"))

;;;; auth

(defun authenticate (database username password)
  (labels ((md5 (text)
             (ironclad:byte-array-to-hex-string
              (ironclad:digest-sequence
               :md5 (babel:string-to-octets text :encoding :utf-8))))
           (nonce-key (nonce username password)
             (format nil
                     "~A~A~A"
                     nonce
                     username
                     (md5 (format nil "~A:mongo:~A" username password)))))
    (maybe-finished
     (alet ((nonce-reply (run-command database "getnonce")))
       (run-command database
                    (son "authenticate" 1
                         "user" username
                         "nonce" (gethash "nonce" nonce-reply)
                         "key" (nonce-key (gethash "nonce" nonce-reply) username password)))))))

(defun logout (database)
  (run-command database "logout"))

;;;; collections

(defun collection-names (database)
  "Get a list of all the collection in this DATABASE."
  (maybe-finished
   (alet ((reply (send-message-and-read-reply
                  (mongo-client database)
                  (make-instance 'op-query
                                 :full-collection-name (format nil "~A.system.namespaces" (database-name database))
                                 :query (son)))))
     (iter (for item in (op-reply-documents reply))
           (let* ((fullname (gethash "name" item))
                  (name (second (multiple-value-list (starts-with-subseq (format nil "~A." (database-name database))
                                                                         fullname
                                                                         :return-suffix t)))))
             (when (and name (not (find #\$ name)))
               (collect name)))))))

(defun create-collection (db name &key size capped max)
  (check-type name string)
  "Create new Collection in DATABASE."
  (let ((cmd (son "create" name)))
    (iter (for (key . value) in `(("size" . ,size) ("cappend" . ,capped) ("max" . ,max)))
          (setf (gethash key cmd) value))
    (run-command db cmd)))

(defun drop-collection (db name)
  (run-command db (son "drop" name)))

;; (defun rename-collection (db name &key drop-target)
;;   )

;;;; eval

(defun eval-js (db code &key args nolock)
  (let ((cmd (son "$eval" (make-instance 'mongo.bson:javascript :code code))))
    (when args
      (setf (gethash "args" cmd) args))
    (when nolock
      (setf (gethash "nolock" cmd) t))
    (maybe-finished
     (alet ((retval (run-command db cmd)))
       (gethash "retval" retval)))))

;;;; misc

(defun stats (db)
  (run-command db "dbStats"))

(defun cursor-info (db)
  (run-command db "cursorInfo"))


;; TODO
;; 
;; renameCollection
;; dereference
;;
;; admin
;; addUser
;; removeUser


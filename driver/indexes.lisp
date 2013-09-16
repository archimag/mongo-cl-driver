;;;; indexes.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver)

(defun make-name (keys)
  (format nil "~{~A_~A~^_~}" (hash-table-plist keys)))

(defun index-collection (collection)
  (collection (collection-database collection)
              "system.indexes"))

(defun create-index (collection keys &optional (options (son)))
  (let ((index (copy-hash-table options))
        (index-collection (index-collection collection))
        (name (make-name keys)))
    (setf (gethash "key" index) keys
          (gethash "ns" index) (fullname collection)
          (gethash "name" index) (gethash "name" index name))
    (insert-op index-collection index)))

(defun ensure-index (collection keys &optional (options (son)))
  ;; Should be cached
  (create-index collection keys options))

(defun drop-indexes (collection &optional (name "*"))
  (let ((cmd (son "dropIndexes" (collection-name collection)
                  "index" name)))
    (maybe-finished
     (alet ((reply (run-command (collection-database collection) cmd)))
       (case (truncate (gethash "ok" reply))
         (0 (values nil (gethash "errmsg" reply)))
         (1 t))))))

(defun reindex (collection)
  (let ((cmd (son "reIndex" (collection-name collection))))
    (maybe-finished
     (alet ((reply (run-command (collection-database collection) cmd)))
       reply))))

(defun index-information (collection name)
  (find-one (index-collection collection)
            :query (son "ns" (fullname collection)
                        "name" name)))

(defun indexes (collection)
  (find-list (index-collection collection)
             :query (son "ns" (fullname collection))))

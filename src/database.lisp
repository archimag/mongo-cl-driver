;;;; database.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver)

(defclass database ()
  ((connection :initarg :connection :reader database-connection)
   (name :initarg :name :reader database-name)))

(defun run-command (db cmd)
  (let ((conn (database-connection db)))
    (send-message conn
                  (make-instance 'op-query
                                 :number-to-return 1
                                 :full-collection-name (format nil "~A.$cmd" (database-name db))
                                 :return-field-selector cmd))
    (first (op-reply-documents (read-reply conn)))))

(defun db-stats (db)
  (run-command db '(("dbStats" . 1))))

(defun get-last-error (db)
  (run-command db '(("getLastError" . 1))))

(defun list-collection-names (db)
  (let ((conn (database-connection db)))
    (send-message conn
                  (make-instance 'op-query
                                 :full-collection-name (format nil "~A.system.namespaces" (database-name db))
                                 :return-field-selector nil))
    (op-reply-documents (read-reply conn))))
  
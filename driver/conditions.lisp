;;;; conditions.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver)

(define-condition mongo-condition (condition)
  ((description :initform "" :initarg :description :reader mongo-condition-description))
  (:documentation "Superclass for all conditions related to mongo-cl-driver."))

(defmethod print-object ((condition mongo-condition) stream)
  (print-unreadable-object (condition stream :type t :identity t)
    (princ (mongo-condition-description condition) stream)))

(define-condition mongo-error (mongo-condition error)
  ()
  (:documentation "Superclass for all errors related to mongo-cl-driver."))

(define-condition connection-failure (mongo-error)
  ()
  (:documentation "Signaled when a connection to the database cannot be made or is lost."))

(define-condition cursor-not-found (mongo-error)
  ()
  (:documentation "Signaled when getMore is called but the cursor id is not valid at the server."))

(define-condition query-failure (mongo-error)
  ()
  (:documentation "Signaled when query failed."))

(define-condition operation-failure (mongo-error)
  ((code :initform nil :initarg :code :reader operation-failure-code))
  (:documentation "Signaled when a database operation fails"))

(defmethod print-object ((err operation-failure) stream)
  (print-unreadable-object (err stream :type t :identity t)
    (format stream
            "~A [~A]"
            (mongo-condition-description err)
            (operation-failure-code err))))

;; (define-condition timeout-failure (operation-failure)
;;   ()
;;   (:documentation "Signaled when a database operation times out."))

(define-condition duplicate-key-error (operation-failure)
  ()
  (:documentation "Signaled when a insert or update fails due to a duplicate key error."))

;; (define-condition invalid-operation (mongo-error)
;;   ()
;;   (:documentation "Signaled when a client attempts to perform an invalid operation."))

;;;; check errors

(defun check-reply-last-error (reply)
  "Check a response"
  (cond
    ((cursor-not-found-p reply)
     (make-condition 'cursor-not-found))
    ((query-failure-p reply)
     (make-condition 'query-failure
                     :description (gethash "$err" (first (op-reply-documents reply)))))
    (t
     (let* ((doc (first (op-reply-documents reply)))
            (errmsg (if doc (gethash "err" doc)))
            (errobjects (if doc (gethash "errObjects" doc)))
            code)
       (when errmsg
         ;; mongos returns the error code in an error object
         ;; for some errors.       
         (iter (for errobj in errobjects)
               (when (string= errmsg (gethash "err" errobj))
                 (setf doc errobj)
                 (return)))
         
         (setf code (gethash "code" doc))

         (if (member code '(11000 11001 12582))
             (make-condition 'duplicate-key-error
                             :description errmsg
                             :code code)
             (make-condition 'operation-failure
                             :description errmsg
                             :code code)))))))

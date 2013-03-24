;;;; cursor.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver)

(defclass cursor ()
  ((id
    :initarg :id
    :initform nil
    :reader cursor-id)
   (collection
    :initarg :collection
    :reader cursor-collection)
   (documents
    :initarg :documents
    :initform nil
    :reader cursor-documents)))

(defmethod mongo-client ((cursor cursor))
  (mongo-client (cursor-collection cursor)))

(defmethod print-object ((cursor cursor) stream)
  (print-unreadable-object (cursor stream :type t :identity t)
    (princ (cursor-id cursor) stream)))

(defun find-cursor (collection &optional query fields)
  (maybe-finished
   (alet ((reply (send-message-and-read-reply
                  (mongo-client collection)
                  (make-instance 'op-query
                                 :full-collection-name (fullname collection)
                                 :query query
                                 :return-field-selector fields))))
     (make-instance 'cursor
                    :id (op-reply-cursor-id reply)
                    :collection collection
                    :documents (op-reply-documents reply)))))

                         
(defun close-cursor (cursor)
  (maybe-finished
   (alet ((reply (send-message (mongo-client cursor)
                               (make-instance 'op-kill-cursors
                                              :cursor-ids (list (cursor-id cursor))))))
     (declare (ignore reply))
     (reinitialize-instance cursor :id nil :collection nil :documents nil)
     cursor)))

(defun refresh-cursor (cursor)
  (maybe-finished
   (alet ((reply (send-message-and-read-reply
                  (mongo-client cursor)
                  (make-instance 'op-getmore
                                 :cursor-id (cursor-id cursor)
                                 :full-collection-name (fullname (cursor-collection cursor))
                                 :number-to-return 20))))
     (setf (slot-value cursor 'documents)
           (op-reply-documents reply))
     cursor)))


(defun iterate-cursor (cursor handler)
  (print "iterate")
  (let ((future (make-future)))
    (labels ((iterate-cursor-impl (&optional x)
               (declare (ignore x))
               (cond
                 ((cursor-documents cursor)
                  (iter (for item in (cursor-documents cursor))
                        (funcall handler item))
                  (handler-case
                      (let ((refresh-future (refresh-cursor cursor)))
                        (attach refresh-future
                                #'iterate-cursor-impl)
                        (attach-errback refresh-future
                                        (lambda (err)
                                          (cond
                                            ((typep err 'cursor-not-found)
                                             (finish future))
                                            (t
                                             (signal-error future err))))))
                    (cursor-not-found ()
                      (finish future))))
                 (t
                  (finish future)))))
      (iterate-cursor-impl nil)
      (maybe-finished
       future))))

(defmacro docursor ((var cursor &key (close-after t)) &body body)
  (if close-after
      (let ((cur (gensym)))
        `(let ((future (make-future)))
           (alet ((,cur ,cursor))
             (wait-for (iterate-cursor ,cur (lambda (,var) ,@body))
               (wait-for (close-cursor ,cur)
                 (finish future)))
             future)))
      `(iterate-cursor ,cursor
                       (lambda (,var) ,@body))))


(defmacro with-cursor ((name collection &optional query fields) &body body)
  `(let ((,name (find-cursor ,collection ,query ,fields)))
     (unwind-protect
          (progn ,@body)
       (close-cursor ,name))))

;;; TODO

;; cursorInfo

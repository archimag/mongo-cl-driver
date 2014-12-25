;;;; cursor.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver)

(defparameter *cursor-batch-size* 0)

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

(defun find-cursor (collection &key query fields)
  (try-unpromisify
   (alet ((reply (send-message-and-read-reply
                  (mongo-client collection)
                  (make-instance 'op-query
                                 :full-collection-name (fullname collection)
                                 :number-to-return *cursor-batch-size*
                                 :query query
                                 :return-field-selector fields))))
     (make-instance 'cursor
                    :id (op-reply-cursor-id reply)
                    :collection collection
                    :documents (op-reply-documents reply)))))

                         
(defun close-cursor (cursor)
  (try-unpromisify
   (alet ((reply (send-message (mongo-client cursor)
                               (make-instance 'op-kill-cursors
                                              :cursor-ids (list (cursor-id cursor))))))
     (declare (ignore reply))
     (reinitialize-instance cursor :id nil :collection nil :documents nil)
     cursor)))

(defun refresh-cursor (cursor)
  (try-unpromisify
   (alet ((reply (send-message-and-read-reply
                  (mongo-client cursor)
                  (make-instance 'op-getmore
                                 :cursor-id (cursor-id cursor)
                                 :full-collection-name (fullname (cursor-collection cursor))
                                 :number-to-return *cursor-batch-size*))))
     (setf (slot-value cursor 'documents)
           (op-reply-documents reply))
     cursor)))


(defun iterate-cursor (cursor handler)
  (try-unpromisify
   (with-promise (resolve reject)
     (labels ((iterate-cursor-impl (&optional x)
                (declare (ignore x))
                (cond
                  ((cursor-documents cursor)
                   (iter (for item in (cursor-documents cursor))
                         (funcall handler item))
                   (catcher
                    (attach (refresh-cursor cursor)
                            #'iterate-cursor-impl)
                    (cursor-not-found (e)
                      (declare (ignore e))
                      (resolve))
                    (t (e)
                       (reject e))))
                  (t
                   (resolve)))))
       (iterate-cursor-impl nil)))))

(defmacro docursor ((var cursor &key (close-after t)) &body body)
  (if close-after
      (let ((cur (gensym)))
        `(let ((promise (make-promise)))
           (alet ((,cur ,cursor))
             (wait-for (iterate-cursor ,cur (lambda (,var) ,@body))
               (wait-for (close-cursor ,cur)
                 (finish promise)))
             promise)))
      `(iterate-cursor ,cursor
                       (lambda (,var) ,@body))))

(defmacro with-cursor-sync ((name collection &key query fields) &body body)
  `(let ((,name (find-cursor ,collection :query ,query :fields ,fields)))
     (unwind-protect
          (progn ,@body)
       (close-cursor ,name))))

(defmacro with-cursor ((name collection &key query fields) &body body)
  `(let (,name)
     (chain (find-cursor ,collection :query ,query :fields ,fields)
       (:attach (x)
         (setf ,name x)
         x)
       (:attach (,name)
         ,@body)
       (:finally ()
         (when ,name
           (close-cursor ,name))))))


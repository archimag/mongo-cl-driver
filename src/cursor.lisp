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

(defmethod connection ((cursor cursor))
  (connection (cursor-collection cursor)))

(defmethod print-object ((cursor cursor) stream)
  (print-unreadable-object (cursor stream :type t :identity t)
    (princ (cursor-id cursor) stream)))
                         
(defun close-cursor (cursor)
  (send-message (connection cursor)
                (make-instance 'op-kill-cursors
                               :cursor-ids (list (cursor-id cursor))))
  (reinitialize-instance cursor
                         :id nil
                         :collection nil
                         :documents nil)
  (values))

(defun refresh-cursor (cursor)
  (let ((conn (connection cursor)))
    (send-message conn
                  (make-instance 'op-getmore
                                 :cursor-id (cursor-id cursor)
                                 :full-collection-name (collection-fullname (cursor-collection cursor))
                                 :number-to-return 20))
    (let ((reply (read-reply conn)))
      (setf (slot-value cursor 'documents)
            (op-reply-documents reply)))))

(defmacro with-cursor ((name collection &optional query) &body body)
  `(let ((,name (find-cursor ,collection ,query)))
     (unwind-protect
          (progn ,@body)
       (close-cursor ,name))))

(defmacro docursor ((var cursor) &body body)
  (let ((cur (gensym)))
    `(let ((,cur ,cursor))
       (iter (while (cursor-documents ,cur))
             (iter (for ,var in (cursor-documents ,cur))
                   ,@body)
             (refresh-cursor ,cur))
       (values))))
  
   
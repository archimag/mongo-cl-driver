;;;; protocol.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver.wire)

(defconstant +op-reply+ 1 "Reply to a client request. responseTo is set")
(defconstant +op-msg+ 1000 "generic msg command followed by a string")
(defconstant +op-update+ 2001 "update document")
(defconstant +op-insert+ 2002 "insert new document")
(defconstant +reserverd+ 2003 "formerly used for OP_GET_BY_OID")
(defconstant +op-query+ 2004 "query a collection")
(defconstant +op-get-more+ 2005 "Get more data from a query. See Cursors")
(defconstant +op-delete+ 2006 "Delete documents")
(defconstant +op-kill-cursors+ 2007 "Tell database client is done with a cursor")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Message Types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defclass msg-header ()
  ((message-length
    :accessor message-length
    :bson-type :int32)
   (request-id
    :initform 0
    :initarg :request-id
    :accessor request-id
    :bson-type :int32)
   (response-to
    :initform 0
    :initarg :response-to
    :accessor response-to
    :bson-type :int32)
   (opcode
    :initarg :opcode
    :accessor opcode
    :bson-type :int32))
  (:slot-order message-length request-id response-to opcode)
  (:metaclass message-class))

(defmacro define-protocol-message (name code &rest slots)
  `(defclass ,name (msg-header)
     ,slots
     (:default-initargs :opcode ,code)
     (:slot-order message-length request-id response-to opcode ,@(mapcar #'car slots))
     (:metaclass message-class)))

(define-protocol-message op-update +op-update+
  (zero
   :initform 0
   :bson-type :int32)
  (full-collection-name
   :initarg :full-collection-name
   :initform ""
   :bson-type :cstring)
  (flags
   :initform 0
   :bson-type :int32)
  (selector
   :initarg :selector
   :bson-type :document)
  (update
   :initarg :update
   :bson-type :document))

(defmethod shared-initialize :after ((msg op-update) slot-names &key upsert multi-update &allow-other-keys)
  (let ((bits nil))
    (when upsert
      (push 0  bits))
    (when multi-update
      (push 1 bits))
    (dolist (bit bits)
      (setf (ldb (byte 1 bit)
                 (slot-value msg 'flags))
            1))))

(define-protocol-message op-insert +op-insert+
  (zero
   :initform 0
   :bson-type :int32)
  (full-collection-name
   :initarg :full-collection-name
   :initform ""
   :bson-type :cstring)
  (documents
   :initarg :documents
   :initform nil
   :bson-type :document
   :list-p t))

(define-protocol-message op-query +op-query+
  (flags
   :initform 0
   :bson-type :int32)
  (full-collection-name
   :initarg :full-collection-name
   :initform ""
   :bson-type :cstring)
  (number-to-skip
   :initarg :number-to-skip
   :initform 0
   :bson-type :int32)
  (number-to-return
   :initarg :number-to-return
   :initform 0
   :bson-type :int32)
  (query
   :initarg :query
   :bson-type :document)
  (return-field-selector
   :initarg :return-field-selector
   :initform nil
   :bson-type :document))

(defmethod shared-initialize :after ((query op-query) slot-names &key
                                     tailable-cursor slave-ok no-cursor-timeout
                                     await-data exhaust partial)
  (unless (slot-value query 'return-field-selector)
    (setf (slot-value query 'return-field-selector)
          (make-hash-table :test 'equal)))
  (let ((bits nil))
    (when tailable-cursor (push 1 bits))
    (when slave-ok (push 2 bits))
    (when no-cursor-timeout (push 4 bits))
    (when await-data (push 5 bits))
    (when exhaust (push 6 bits))
    (when partial (push 7 bits))

    (dolist (bit bits)
      (setf (ldb (byte 1 bit)
                 (slot-value query 'flags))
            1))))

(define-protocol-message op-getmore +op-get-more+
  (zero
   :initform 0
   :bson-type :int32)
  (full-collection-name
   :initarg :full-collection-name
   :initform ""
   :bson-type :cstring)
  (number-to-return
   :initarg :number-to-return
   :bson-type :int32)
  (cursor-id
   :initarg :cursor-id
   :bson-type :int64))

(define-protocol-message op-delete +op-delete+
  (zero
   :initform 0
   :bson-type :int32)
  (full-collection-name
   :initarg :full-collection-name
   :initform ""
   :bson-type :cstring)
  (flags
   :initform 0
   :bson-type :int32)
  (selector
   :initarg :selector
   :bson-type :document))

(defmethod shared-initialize :after ((msg op-delete) slot-names &key single-remove &allow-other-keys)
  (when single-remove
    (setf (ldb (byte 1 0)
               (slot-value msg 'flags))
          1)))

(define-protocol-message op-kill-cursors +op-kill-cursors+
  (zero
   :initform 0
   :bson-type :int32)
  (number-of-cursor-ids
   :bson-type :int32)
  (cursor-ids
   :initform nil
   :initarg :cursor-ids
   :bson-type :int64
   :list-p t))

(defmethod shared-initialize :after ((msg op-kill-cursors) slot-names &key &allow-other-keys)
  (setf (slot-value msg 'number-of-cursor-ids)
        (length (slot-value msg 'cursor-ids))))

(define-protocol-message op-reply +op-reply+
  (response-flags
   :reader op-reply-response-flags
   :bson-type :int32)
  (cursor-id
   :reader op-reply-cursor-id
   :bson-type :int64)
  (starting-from
   :reader op-reply-starting-from
   :bson-type :int32)
  (number-returned
   :reader op-reply-number-returned
   :bson-type :int32)
  (documents
   :reader op-reply-documents
   :initform nil
   :bson-type :document
   :list-p t))

(defmacro define-reply-flag-predicate (name bitnum)
  `(defun ,name (reply)
     (= (ldb (byte 1 ,bitnum)
             (op-reply-response-flags reply))
        1)))

(define-reply-flag-predicate cursor-not-found-p 0)
(define-reply-flag-predicate query-failure-p 1)
(define-reply-flag-predicate await-capable-p 3)

(defun check-reply (reply)
  (when (cursor-not-found-p reply)
    (error "Cursor '~A' not found" (op-reply-cursor-id reply)))
  (when (query-failure-p reply)
    (error (gethash "$err" (car (op-reply-documents reply)))))
  reply)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; encode protocol message
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defgeneric encode-protocol-message (message target)
  (:documentation "Serialize MESSAGE to TARGET"))

(defmethod encode-protocol-message (message (target (eql :vector)))
  (encode-protocol-message message
                           (make-array 0
                                       :element-type '(unsigned-byte 8)
                                       :fill-pointer 0
                                       :adjustable t)))

(defmethod encode-protocol-message (message (target (eql :list)))
  (coerce  (encode-protocol-message message :vector)
           'list))

(defmethod encode-protocol-message (message (target (eql :brigade)))
  (encode-protocol-message message
                           (make-instance 'brigade)))

(defmethod encode-protocol-message :around (message target)
  (let ((*encoded-bytes-count* 0))
    (call-next-method)))

(defmethod encode-protocol-message (message target)
  (let ((size (with-count-encoded-bytes
                (dotimes (i 4)
                  (encode-byte 0 target))
                (iter (for slot in (cdr (class-slots (class-of message))))
                      (for value = #-lispworks (slot-value-using-class (class-of message)
                                                                       message
                                                                       slot)
                                   #+lispworks (slot-value-using-class (class-of message)
                                                                       message
                                                                       (slot-definition-name slot)))
                      (for encoder = (message-effective-slot-encoder slot))
                      (funcall encoder value target))))
        (arr (make-array 4 :element-type '(unsigned-byte 8) :fill-pointer 0)))
    (encode-int32 size arr)
    (bson-target-replace target arr 0)
    target))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; decode server reply
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun decode-op-reply (source)
  (let* ((reply (make-instance 'op-reply))
         (reply-class (class-of reply))
         (*decoded-bytes-count* 0))
    (iter (for slot in (butlast (class-slots reply-class)))
          (setf #-lispworks (slot-value-using-class reply-class reply slot)
                #+lispworks (slot-value-using-class reply-class reply (slot-definition-name slot))
                (funcall (message-effective-slot-decoder slot)
                         source)))
    (iter (for i from 0 below (op-reply-number-returned reply))
          (push (decode-document source)
                (slot-value reply 'documents)))
    (setf (slot-value reply 'documents)
          (nreverse (slot-value reply 'documents)))
    reply))

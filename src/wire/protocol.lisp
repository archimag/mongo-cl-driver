;;;; wire.lisp
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
;;;; message-class metaclass
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defclass message-class (standard-class)
  ((slot-order :initform ()
               :initarg :slot-order
               :reader class-slot-order)))

(defmethod compute-slots ((class message-class))
  (let ((order (class-slot-order class)))
    (sort (copy-list (call-next-method))
          #'(lambda (a b)
              (< (position (slot-definition-name a) order)
                 (position (slot-definition-name b) order))))))
                     
(defmethod validate-superclass ((sub message-class) (super standard-class))
  t)

(defclass message-slot-definition (standard-direct-slot-definition)
  ((bson-type :initarg :bson-type :reader message-slot-definition-bson-type)
   (list-p :initarg :list-p :initform nil :reader message-slot-definition-list-p)))

(defclass message-effective-slot-definition (standard-effective-slot-definition)
  ((encoder :initform nil :initarg :encoder :reader message-effective-slot-encoder)
   (decoder :initform nil :initarg :decoder :reader message-effective-slot-decoder)))

(defmethod direct-slot-definition-class ((class message-class) &key)
  'message-slot-definition)

(defmethod effective-slot-definition-class ((class message-class) &rest initargs)
  (declare (ignore initargs))
  'message-effective-slot-definition)

(defmethod compute-effective-slot-definition ((class message-class)  name direct-slots)
  (let* ((normal-slot (call-next-method))
         (direct-slot (find name
                             direct-slots
                             :key #'slot-definition-name))
         (bson-type (message-slot-definition-bson-type direct-slot))
         (list-p (message-slot-definition-list-p direct-slot))
         (encoder (or (find-symbol (format nil "ENCODE-~A" bson-type)
                           '#:mongo-cl-driver.bson)
                      (error "Can not find a encoder for ~A type" bson-type)))
         (decoder (or (find-symbol (format nil "DECODE-~A" bson-type)
                           '#:mongo-cl-driver.bson)
                      (error "Can not find a decoder for ~A type" bson-type))))
    
    (setf (slot-value normal-slot 'encoder)
          (if list-p
              #'(lambda (value target)
                  (iter (for item in value)
                        (funcall encoder item target)))
              encoder))
    (setf (slot-value normal-slot 'decoder)
          decoder)
    normal-slot))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Message Types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defclass msg-header ()
  ((message-length
    :accessor message-length
    :bson-type :int32)
   (request-id
    :initform 0
    :accessor request-id
    :bson-type :int32)
   (response-to
    :accessor response-to
    :initform 0
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
   :initarg :flags
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

(defmethod encode-protocol-message :around (message target)
  (let ((*encoded-bytes-count* 0))
    (call-next-method)))

(defmethod encode-protocol-message (message target)
  (let ((size (with-count-encoded-bytes
                (dotimes (i 4)
                  (encode-byte 0 target))
                (iter (for slot in (cdr (class-slots (class-of message))))
                      (for value = (slot-value-using-class (class-of message)
                                                           message
                                                           slot))
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
          (setf (slot-value-using-class reply-class reply slot)
                (funcall (message-effective-slot-decoder slot)
                         source)))
    (iter (for i from 0 below (op-reply-number-returned reply))
          (push (decode-document source)
                (slot-value reply 'documents)))
    reply))
          
          

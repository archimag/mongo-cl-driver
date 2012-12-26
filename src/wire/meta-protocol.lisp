;;;; meta-protocol.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver.wire)

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

(defmethod direct-slot-definition-class ((class message-class) &rest initargs)
  (declare (ignore initargs))
  'message-slot-definition)

(defmethod effective-slot-definition-class ((class message-class) &rest initargs)
  (declare (ignore initargs))
  'message-effective-slot-definition)

(defmethod compute-effective-slot-definition ((class message-class) name direct-slots)
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

;;;; bson.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver.bson)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Terminals
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defvar *encoded-bytes-count*)

(defgeneric encode-byte (byte target)
  (:documentation "Encode BYTE to TARGET"))

(defmacro with-count-encoded-bytes (&body body)
  (let ((encode-bytes-count (gensym)))
    `(let ((,encode-bytes-count *encoded-bytes-count*))
       ,@body
       (- *encoded-bytes-count* ,encode-bytes-count))))

(defgeneric bson-target-replace (target sequence start)
  (:documentation "Destructively modifies target by replacing the elements of target from start  with the elements of subsequence"))

(defmethod bson-target-replace ((target vector) sequence start)
  (replace target sequence :start1 start))

(defmethod encode-byte :around (byte target)
  (check-type byte (unsigned-byte 8))
  (call-next-method)
  (incf *encoded-bytes-count*))

(defmethod encode-byte (byte (target vector))
  (vector-push-extend byte target))

(defun encode-int32 (i32 target)
  "Encode 32-bit integer to TARGET"
  (iter (for i from 0 below 32 by 8)
        (encode-byte (ldb (byte 8 i) i32)
                     target)))

(defun encode-int64 (i64 target)
  "Encode 64-bit integer to TARGET"
  (iter (for i from 0 below 64 by 8)
        (encode-byte (ldb (byte 8 i) i64) target)))

(defun encode-double (double target)
  "Encode 64-bit IEEE 754 floating point to TARGE"
  (encode-int64 (ieee-floats:encode-float64 double)
                target))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Non-terminals
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun encode-string (string target)
  "Encode BSTR-style string"
  (let ((octets (babel:string-to-octets string :encoding :utf-8)))
    (encode-int32 (1+ (length octets)) target)
    (iter (for octet in-vector octets)
          (encode-byte octet target))
    (encode-byte #x00 target)))

(defun encode-cstring (string target)
  "Encode C-style string"
  (let ((octets (babel:string-to-octets string :encoding :utf-8)))
    (iter (for octet in-vector octets)
          (encode-byte octet target))
    (encode-byte #x00 target)))

(defun encode-ename (name target)
  (encode-cstring name target))

(defun encode-boolean (flag target)
  (encode-byte (if flag #x01 #x00)
               target))

(defun encode-array (array target
                     &aux (index *encoded-bytes-count*))
  (let ((count (with-count-encoded-bytes
                   (dotimes (i 4)
                     (encode-byte 0 target))
                   (let ((pos -1))
                     (map 'nil
                          (lambda (el)
                            (encode-element (write-to-string (incf pos))
                                            el
                                            target))
                          array))
                   (encode-byte #x00 target)))
        (arr (make-array 4 :element-type '(unsigned-byte 8) :fill-pointer 0))
        (*encoded-bytes-count* 0))
    (encode-int32 count arr)
    (bson-target-replace target arr index)))

(defun encode-object-id (id target)
  (iter (for byte in-vector (slot-value id 'raw))
        (encode-byte byte target)))

(defun encode-utc-datetime (timestamp target)
  (encode-int64 (* (local-time:timestamp-to-unix timestamp) 1000)
                target))

(defun encode-binary-data (binary target)
  (encode-int32 (length (binary-data-octets binary)) target)
  (encode-byte (case (binary-data-subtype binary)
                 (:generic      #x00)
                 (:function     #x01)
                 (:uuid         #x03)
                 (:md5          #x05)
                 (:user-defined #x80))
               target)
  (iter (for byte in-vector (binary-data-octets binary))
        (encode-byte byte target)))

(defun encode-regex (regex target)
  (encode-cstring (regex-pattern regex) target)
  (encode-cstring (regex-options regex) target))

(defun encode-javascript (js target &aux (index *encoded-bytes-count*))
  (let ((count (prog1 (with-count-encoded-bytes
                        (dotimes (i 4)
                          (encode-byte 0 target))
                        (encode-string (javascript-code js) target)
                        (encode-document (javascript-scope js) target))))
        (arr (make-array 4 :element-type '(unsigned-byte 8) :fill-pointer 0))
        (*encoded-bytes-count* 0))
    (encode-int32 count arr)
    (bson-target-replace target arr index)))

(defun encode-mongo-timestamp (timestamp target)
  (encode-int64 (mongo-timestamp-value timestamp)
                target))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Document
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defgeneric encode-element (key value target)
  (:documentation "Encode pair KEY/VALUE to target"))

(defmethod encode-element (key (value (eql t)) target)
  (encode-byte #x08 target)
  (encode-ename key target)
  (encode-boolean value target))

(defmethod encode-element (key (value (eql nil)) target)
  (encode-byte #x08 target)
  (encode-ename key target)
  (encode-boolean value target))

(defmethod encode-element (key (value (eql +min-key+)) target)
  (encode-byte #xFF target)
  (encode-ename key target))

(defmethod encode-element (key (value (eql +max-key+)) target)
  (encode-byte #x7F target)
  (encode-ename key target))

(defmethod encode-element (key (value integer) target)
  (check-type value (integer #x-8000000000000000 #x7FFFFFFFFFFFFFFF))
  (cond
    ((and (>= value #x-80000000) (<= value #x7FFFFFFF))
     (encode-byte #x10 target)
     (encode-ename key target)
     (encode-int32 value target))

    (t
     (encode-byte #x12 target)
     (encode-ename key target)
     (encode-int64 value target))))
     
(defmethod encode-element (key (value number) target)
  (encode-byte #x01 target)
  (encode-ename key target)
  (encode-double value target))

(defmethod encode-element (key (value string) target)
  (encode-byte #x02 target)
  (encode-ename key target)
  (encode-string value target))

(defmethod encode-element (key (value vector) target)
  (encode-byte #x04 target)
  (encode-ename key target)
  (encode-array value target))

(defmethod encode-element (key (value list) target)
  (encode-byte #x04 target)
  (encode-ename key target)
  (encode-array value target))

(defmethod encode-element (key (value hash-table) target)
  (encode-byte #x03 target)
  (encode-ename key target)
  (encode-document value target))

(defmethod encode-element (key (value object-id) target)
  (encode-byte #x07 target)
  (encode-ename key target)
  (encode-object-id value target))

(defmethod encode-element (key (value local-time:timestamp) target)
  (encode-byte #x09 target)
  (encode-ename key target)
  (encode-utc-datetime value target))

(defmethod encode-element (key (value binary-data) target)
  (encode-byte #x05 target)
  (encode-ename key target)
  (encode-binary-data value target))

(defmethod encode-element (key (value regex) target)
  (encode-byte #x0B target)
  (encode-ename key target)
  (encode-regex value target))

(defmethod encode-element (key (value javascript) target)
  (encode-byte #x0F target)
  (encode-ename key target)
  (encode-javascript value target))

(defmethod encode-element (key (value mongo-timestamp) target)
  (encode-byte #x11 target)
  (encode-ename key target)
  (encode-mongo-timestamp value target))

(defun encode-document (document target)
  "Encode DOCUMENT as BSON to target"
  (check-type document hash-table)
  (let* ((index *encoded-bytes-count*)
         (count (prog1 (with-count-encoded-bytes
                         (dotimes (i 4)
                           (encode-byte 0 target))
                         (iter (for (key value) in-hashtable document)
                               (encode-element key value target))
                         (encode-byte #x00 target))))
         (arr (make-array 4 :element-type '(unsigned-byte 8) :fill-pointer 0))
         (*encoded-bytes-count* 0))
    (encode-int32 count arr)
    (bson-target-replace target arr index)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; encode
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defgeneric encode (obj target)
  (:documentation "Encode OBJ to TARGET"))

(defmethod encode (obj target)
  (let ((*encoded-bytes-count* 0))
    (encode-document obj target)))

(defmethod encode (obj (target (eql :vector)))
  (let ((vector (make-array 0 :element-type 'ub8 :fill-pointer 0 :adjustable t)))
    (encode obj vector)
    vector))

(defmethod encode (obj (target (eql :list)))
  (coerce (encode obj :vector)
          'list))

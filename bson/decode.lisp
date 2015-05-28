;;;; bson.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver.bson)

(define-condition decode-simple-error (simple-condition) ())

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Terminals
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defvar *decoded-bytes-count*)

(defgeneric decode-byte (source)
  (:documentation "Decode byte from SOURCE"))

(defmethod decode-byte :after (source)
  (incf *decoded-bytes-count*))

(defmethod decode-byte ((source stream))
  (read-byte source))

(defmethod decode-byte ((source vector))
  (aref source *decoded-bytes-count*))

(defun decode-int32 (source)
  "Decode 32-bit signed integer from SOURCE"
  (let ((ui32 0))
    (iter (for i from 0 below 32 by 8)
          (setf (ldb (byte 8 i) ui32)
                (decode-byte source)))
    (if (logbitp 31 ui32)
        (1- (- (logandc2 #xFFFFFFFF ui32)))
        ui32)))

(defun decode-int64 (source)
  "Decode 64-bit signed integer from SOURCE"
  (let ((ui64 0))
    (iter (for i from 0 below 64 by 8)
          (setf (ldb (byte 8 i) ui64)
                (decode-byte source)))
    (if (logbitp 63 ui64)
        (1- (- (logandc2 #xFFFFFFFFFFFFFFFF ui64)))
        ui64)))

(defun decode-uint64 (source)
  "Decode 64-bit unsigned integer from SOURCE"
  (let ((ui64 0))
    (iter (for i from 0 below 64 by 8)
          (setf (ldb (byte 8 i) ui64)
                (decode-byte source)))
    ui64))

(defun decode-double (source)
  "Decode 64-bit IEEE 754 floating point from SOURCE"
  (ieee-floats:decode-float64 (decode-uint64 source)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Non-terminals
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun decode-string (source)
  "Decode BSTR-style string"
  (let* ((count (1- (decode-int32 source)))
         (octets (make-array count :element-type '(unsigned-byte 8))))
    (iter (for i from 0 below count)
          (for byte = (decode-byte source))
          (setf (aref octets i)
                byte))
    (unless (= (decode-byte source) #x00)
      (error "Bad format: not #x00 byte in end string"))
    (babel:octets-to-string octets :encoding :utf-8)))

(defun decode-cstring (source)
  "Decode C-style string"
  (let ((octets (make-array 0
                            :element-type '(unsigned-byte 8)
                            :fill-pointer 0
                            :adjustable t)))
    (iter (for octet = (decode-byte source))
          (while (not (= octet 0)))
          (vector-push-extend octet octets))
    (babel:octets-to-string octets :encoding :utf-8)))

(defun decode-ename (source)
  (decode-cstring source))

(defun decode-boolean (source &aux (byte (decode-byte source)))
  (values (case byte
            (#x01 t)
            (#x00 nil)
            (otherwise (error "Bad format: ~A is not boolean byte" byte)))))

(defun decode-object-id (source)
  (declare (optimize (debug 3)))
  (let* ((id (make-instance 'object-id))
         (raw (slot-value id 'raw)))
    (iter (for i from 0 below 12)
          (setf (aref raw i)
                (decode-byte source)))
    id))

(defun decode-array (source)
  (declare (optimize (debug 3)))
  (let ((end (+ *decoded-bytes-count*
                 (decode-int32 source)
                 -1)))
    (prog1 
        (iter (for i from 0)
              (while (< *decoded-bytes-count* end))
              (let ((key-value (decode-element source)))
                (unless (= i (if (stringp (car key-value))
                                 (parse-integer (car key-value))
                                 (car key-value)))
                  (error "Bad format: ~A is not equal ~A" (car key-value) i))
                (collect (cdr key-value))))
      (unless (= end *decoded-bytes-count*)
        (error "Bad format"))
      (decode-byte source))))

(defun decode-utc-dateime (source)
  (local-time:unix-to-timestamp (floor (decode-int64 source) 1000)))

(defun decode-binary-data (source)
  (let* ((size (decode-int32 source))
         (subtype (decode-byte source))
         (data (make-array size :element-type 'ub8)))
    (dotimes (i size)
      (setf (aref data i)
            (decode-byte source)))
    (make-instance 'binary-data
                   :subtype (case subtype
                              ((#x00 #x02) :generic)
                              (#x01        :function)
                              (#x03        :uuid)
                              (#x05        :md5)
                              (#x80        :user-defined))
                   :octets data)))

(defun decode-regex (source)
  (make-instance 'regex
                 :pattern (decode-cstring source)
                 :options (decode-cstring source)))

(defun decode-javascript (source)
  (let ((end (+ *decoded-bytes-count* (decode-int32 source)))
        (code (decode-string source)))
    (unless (= end *decoded-bytes-count*)
      (error "Bad format"))
    (make-instance 'javascript
                   :code code)))

(defun decode-javascript-w-scope (source)
  (let ((end (+ *decoded-bytes-count* (decode-int32 source)))
        (code (decode-string source))
        (scope (decode-document source)))
    (unless (= end *decoded-bytes-count*)
      (error "Bad format"))
    (make-instance 'javascript
                   :code code
                   :scope scope)))        

(defun decode-mongo-timestamp (source)
  (make-instance 'mongo-timestamp
                 :value (decode-int64 source)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Document
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun decode-element (source)
  (declare (optimize (debug 3)))
  (flet ((constant-decoder (val)
           (lambda (src)
             (declare (ignore src))
             val))
         (unimplemented-decoder (code)
           (error "Decoder for code '#x~2,'0X' unimplemented" code)))
    (let* ((code (decode-byte source))
           (decoder (case code
                      (#x01 #'decode-double)
                      (#x02 #'decode-string)
                      (#x03 #'decode-document)
                      (#x04 #'decode-array)
                      (#x05 #'decode-binary-data)
                      (#x06 (constant-decoder :undefined))
                      (#x07 #'decode-object-id)
                      (#x08 #'decode-boolean)
                      (#x09 #'decode-utc-dateime)
                      (#x0A (constant-decoder nil))
                      (#x0B #'decode-regex)
                      (#x0C (unimplemented-decoder #x0C))
                      (#x0D #'decode-javascript)
                      (#x0E (unimplemented-decoder #x0E))
                      (#x0F #'decode-javascript-w-scope)
                      (#x10 #'decode-int32)
                      (#x11 #'decode-mongo-timestamp)
                      (#x12 #'decode-int64)
                      (#xFF (constant-decoder +min-key+))
                      (#x7F (constant-decoder +max-key+))
                      (otherwise (unimplemented-decoder code)))))
      (cons (decode-ename source)
            (funcall decoder source)))))

(defun decode-document (source)
  "Decode BSON document from SOURCE"
  (let ((end (+ *decoded-bytes-count* (decode-int32 source) -1))
        (son (make-hash-table :test 'equal)))
    (iter (while (< *decoded-bytes-count* end))
          (for (key . value) = (decode-element source))
          (setf (gethash key son)
                value))
    (unless (= end *decoded-bytes-count*)
      (error "Bad format"))
    (unless (= (decode-byte source) #x00)
      (error "Bad format"))
    son))

    
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; decode
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun decode (source)
  "Decode BSON object from SOURCE"
  (let ((*decoded-bytes-count* 0))
    (decode-document source)))

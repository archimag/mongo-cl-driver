;;;; bson.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver.bson)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; BSON types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defconstant +min-key+ '+min-key+)
(defconstant +max-key+ '+max-key+)

(defclass object-id ()
  ((raw :initform (make-array 12 :element-type '(unsigned-byte 8)))))

(defmethod shared-initialize :after ((id object-id) slot-names &key raw)
  (when raw
    (replace (slot-value id 'raw) raw)))

(defmethod print-object ((id object-id) stream)
  (print-unreadable-object (id stream :type t :identity nil)
    (iter (for ch in-vector (slot-value id 'raw))
          (format stream "~2,'0X" ch))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; BSON target minimal interface
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defgeneric bson-target-replace (target sequence start)
  (:documentation "Destructively modifies target by replacing the elements of target from start  with the elements of subsequence"))

(defmethod bson-target-replace ((target vector) sequence start)
  (replace target sequence :start1 start))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Encode/decode generic methods
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defvar *encoded-bytes-count*)
(defvar *decoded-bytes-count*)

(defgeneric encode-byte (byte target)
  (:documentation "Encode BYTE to TARGET"))

(defmethod encode-byte :around (byte target)
  (check-type byte (unsigned-byte 8))
  (call-next-method)
  (incf *encoded-bytes-count*))

(defmethod encode-byte (byte (target vector))
  (check-type target (and (vector (unsigned-byte 8)) (not simple-array)))
  (vector-push-extend byte target))

(defgeneric decode-byte (source)
  (:documentation "Decode byte from SOURCE"))

(defmethod decode-byte :after (source)
  (incf *decoded-bytes-count*))

(defmethod decode-byte ((source stream))
  (read-byte source))

(defmethod decode-byte ((source vector))
  (aref source *decoded-bytes-count*))

(defmacro with-count-encoded-bytes (&body body)
  (let ((encode-bytes-count (gensym)))
    `(let ((,encode-bytes-count *encoded-bytes-count*))
       ,@body
       (- *encoded-bytes-count* ,encode-bytes-count))))
  
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Basic types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun encode-int32 (i32 target)
  "Encode 32-bit integer to TARGET"
  (iter (for i from 0 below 32 by 8)
        (encode-byte (ldb (byte 8 i) i32)
                     target)))

(defun decode-int32 (source)
  "Decode 32-bit signed integer from SOURCE"
  (let ((ui32 0))
    (iter (for i from 0 below 32 by 8)
          (setf (ldb (byte 8 i) ui32)
                (decode-byte source)))
    (if (logbitp 31 ui32)
        (1- (- (logandc2 #xFFFFFFFF ui32)))
        ui32)))

(defun encode-int64 (i64 target)
  "Encode 64-bit integer to TARGET"
  (iter (for i from 0 below 64 by 8)
        (encode-byte (ldb (byte 8 i) i64) target)))

(defun decode-int64 (source)
  "Decode 64-bit signed integer from SOURCE"
  (let ((ui64 0))
    (iter (for i from 0 below 64 by 8)
          (setf (ldb (byte 8 i) ui64)
                (decode-byte source)))
    (if (logbitp 63 ui64)
        (1- (- (logandc2 #xFFFFFFFFFFFFFFFF ui64)))
        ui64)))

(defun encode-double (double source)
  (encode-int64 (ieee-floats:encode-float64 double)
                source))

(defun decode-double (source)
  "Decode 64-bit IEEE 754 floating point from SOURCE"
  (ieee-floats:decode-float64 (decode-int64 source)))
    
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; strings
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun encode-string (string target)
  "Encode BSTR-style string"
  (let ((octets (babel:string-to-octets string :encoding :utf-8)))
    (encode-int32 (1+ (length octets)) target)
    (iter (for octet in-vector octets)
          (encode-byte octet target))
    (encode-byte #x00 target)))

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

(defun encode-cstring (string target)
  "Encode C-style string"
  (let ((octets (babel:string-to-octets string :encoding :utf-8)))
    (iter (for octet in-vector octets)
          (encode-byte octet target))
    (encode-byte #x00 target)))

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; ename
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defparameter *lisp-identifier-name-to-bson* #'camel-case:lisp-to-camel-case)
;;(defparameter *bson-identifier-name-to-lisp* #'camel-case:camel-case-to-lisp)
(defparameter *bson-identifier-name-to-lisp* nil)

(defun encode-ename (name target)
  (typecase name
    (string (encode-cstring name target))
    (keyword (encode-cstring (funcall *lisp-identifier-name-to-bson*
                                      (symbol-name name))
                             target))
    (otherwise (error "Bad type of ~A" name))))

(defun decode-ename (source)
  (if *bson-identifier-name-to-lisp*
      (intern (funcall *bson-identifier-name-to-lisp*
                       (decode-cstring source))
              :keyword)
      (decode-cstring source)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; boolean
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun encode-boolean (flag target)
  (encode-byte (if flag #x01 #x00)
               target))

(defun decode-boolean (source &aux (byte (decode-byte source)))
  (values (case byte
            (#x01 t)
            (#x00 nil)
            (otherwise (error "Bad format: ~A is not boolean byte" byte)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; object-id
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun encode-object-id (id target)
  (iter (for byte in-vector (slot-value id 'raw))
        (encode-byte byte target)))

(defun decode-object-id (source)
  (declare (optimize (debug 3)))
  (let* ((id (make-instance 'object-id))
         (raw (slot-value id 'raw)))
    (iter (for i from 0 below 12)
          (setf (aref raw i)
                (decode-byte source)))
    id))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; array
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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

(defun decode-array (source)
  (declare (optimize (debug 3)))
  (let ((end (+ *decoded-bytes-count*
                 (decode-int32 source)
                 -1)))
    (prog1 
        (iter (for i from 0)
              (while (< *decoded-bytes-count* end))
              (let ((key-value (decode-element source :identifier-to-lisp nil)))
                (unless (= i (if (stringp (car key-value))
                                 (parse-integer (car key-value))
                                 (car key-value)))
                  (error "Bad format: ~A is not equal ~A" (car key-value) i))
                (collect (cdr key-value))))
      (unless (= end *decoded-bytes-count*)
        (error "Bad format"))
      (decode-byte source))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; UTC datetime
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun encode-utc-datetime (timestamp target)
  (encode-int64 (* (local-time:timestamp-to-unix timestamp) 1000)
                target))

(defun decode-utc-dateime (source)
  (local-time:unix-to-timestamp (floor (decode-int64 source) 1000)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; document
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmacro for-each-element (obj (key value) &body body)
  (let ((doc (gensym))
        (item (gensym)))
    `(let ((,doc ,obj))
       (typecase ,doc
         (hash-table (iter (for (,key ,value) in-hashtable ,doc)
                           ,@body))
         (cons (cond
                 ((keywordp (car ,doc))
                  (iter (for ,item on ,doc by #'cddr)
                        (let ((,key (first ,item))
                              (,value (second ,item)))
                          ,@body)))
                 (t (iter (for (,key . ,value) in ,doc)
                          ,@body))))))))

(defun encode-document (obj target
                        &aux (index *encoded-bytes-count*))
   "Encode OBJ as BSON to target"
   (let ((count (prog1 (with-count-encoded-bytes
                         (dotimes (i 4)
                           (encode-byte 0 target))
                         (for-each-element obj (key value)
                           (encode-element key value target))
                         (encode-byte #x00 target))))
         (arr (make-array 4 :element-type '(unsigned-byte 8) :fill-pointer 0))
         (*encoded-bytes-count* 0))
     (encode-int32 count arr)
     (bson-target-replace target arr index)))

(defun decode-document-to-alist (source end)
  (iter (while (< *decoded-bytes-count* end))
        (collect (decode-element source))))

(defun decode-document-to-plist (source end)
  (iter (while (< *decoded-bytes-count* end))
        (for (key . value) = (decode-element source))
        (collect key)
        (collect value)))

(defun decode-document-to-hashtable (source end)
  (iter (with hash = (make-hash-table :test 'equal))
        (while (< *decoded-bytes-count* end))
        (for (key . value) = (decode-element source))
        (setf (gethash key hash) value)
        (finally (return hash))))

(defparameter *convert-bson-document-to-lisp* #'decode-document-to-hashtable)

(defun decode-document (source)
  "Decode BSON document from SOURCE"
  (let* ((end (+ *decoded-bytes-count*
                 (decode-int32 source)
                 -1)))
    (prog1
        (funcall *convert-bson-document-to-lisp*
                 source
                 end)      
      (unless (= end *decoded-bytes-count*)
        (error "Bad format"))
      (decode-byte source))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; elements
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

(defmethod encode-element (key (value integer) target)
  (cond
    ((and (>= value #x-80000000)
          (<= value #x7FFFFFFF))
     (encode-byte #x10 target)
     (encode-ename key target)
     (encode-int32 value target))

    ((and (>= value #x-8000000000000000)
          (<= value #x7FFFFFFFFFFFFFFF))
     (encode-byte #x12 target)
     (encode-ename key target)
     (encode-int64 value target))

    (t (encode-element key
                       (coerce value 'double-float)
                       target))))
     
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

(defmethod encode-element (key (value hash-table) target)
  (encode-byte #x03 target)
  (encode-ename key target)
  (encode-document value target))

(defmethod encode-element (key (value list) target)
  (cond
    ((or (keywordp (car value))
         (keywordp (caar value)))
     (encode-byte #x03 target)
     (encode-ename key target)
     (encode-document value target))
    
    (t
     (encode-byte #x04 target)
     (encode-ename key target)
     (encode-array value target))))

(defmethod encode-element (key (value object-id) target)
  (encode-byte #x07 target)
  (encode-ename key target)
  (encode-object-id value target))

(defmethod encode-element (key (value local-time:timestamp) target)
  (encode-byte #x09 target)
  (encode-ename key target)
  (encode-utc-datetime value target))

(defun decode-element (source &key (identifier-to-lisp *bson-identifier-name-to-lisp*))
  (declare (optimize (debug 3)))
  (flet ((constant-decoder (val)
           (lambda (src)
             (declare (ignore src))
             val))
         (unimplemented (src)
           (declare (ignore src))
           (error "Unimplemented decoder (")))
    (let* ((code (decode-byte source))
           (decoder (case code
                     (#x01 'decode-double)
                     (#x02 'decode-string)
                     (#x03 'decode-document)
                     (#x04 'decode-array)
                     (#x05 #'unimplemented)
                     (#x06 (constant-decoder :undefined))
                     (#x07 'decode-object-id)
                     (#x08 'decode-boolean)
                     (#x09 'decode-utc-dateime)
                     (#x0A (constant-decoder nil))
                     (#x0B #'unimplemented)
                     (#x0C #'unimplemented)
                     (#x0D #'unimplemented)
                     (#x0E #'unimplemented)
                     (#x0F #'unimplemented)
                     (#x10 #'decode-int32)
                     (#x11 #'unimplemented)
                     (#x12 #'decode-int64)
                     (#xFF #'unimplemented)
                     (#x7F #'unimplemented))))
      (cons (let ((*bson-identifier-name-to-lisp* identifier-to-lisp))
              (decode-ename source))
            (funcall decoder source)))))
    
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; main interface
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defgeneric encode (obj target)
  (:documentation "Encode OBJ to TARGET"))

(defmethod encode (obj target)
  (let ((*encoded-bytes-count* 0))
    (encode-document obj target)))

(defmethod encode (obj (target (eql :vector)))
  (let ((vector (make-array 0
                            :element-type '(unsigned-byte 8)
                            :fill-pointer 0
                            :adjustable t)))
    (encode obj vector)
    vector))

(defmethod encode (obj (target (eql :list)))
  (coerce (encode obj :vector)
          'list))

(defmethod encode (obj (target (eql :debug-string)))
  (with-output-to-string (out)
    (iter (for byte in-vector (encode obj :vector))
          (format out "\\x~2,'0X" byte))))
  
(defgeneric decode (source)
  (:documentation "Decode BSON object from SOURCE"))

(defmethod decode :around (source)
  (let ((*decoded-bytes-count* 0))
    (call-next-method)))

(defmethod decode (source)
  (decode-document source))
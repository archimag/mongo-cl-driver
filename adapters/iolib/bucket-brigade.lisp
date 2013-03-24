;;;; bucket-brigade.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver.iolib)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; bucket pool
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftype bucket (size)
  `(simple-array (ub8) (,size)))

(defgeneric push-bucket (bucket pool)
  (:documentation "Push BUCKET to the POOL"))

(defgeneric pop-bucket (pool)
  (:documentation "Pop bucket from the POOL"))

(defclass bucket-pool ()
  ((buckets :initform nil)
   (bucket-size :initform 4096 :initarg :bucket-size)))

(declaim (inline bucket-pool-typespec))
(defun bucket-pool-typespec (pool)
  `(bucket ,(slot-value pool 'bucket-size)))
;;  `(simple-array (ub8) (,(slot-value pool 'bucket-size))))

(defmethod push-bucket (bucket (pool bucket-pool))
  (let ((typespec (bucket-pool-typespec pool)))
    (assert (typep bucket typespec) (bucket)
            'type-error :datum bucket :expected-type typespec))
  (push bucket
        (slot-value pool 'buckets)))

(defmethod pop-bucket ((pool bucket-pool))
  (or (pop (slot-value pool 'buckets))
      (make-array (slot-value pool 'bucket-size)
                  :element-type 'ub8
                  #+lispworks #+lispworks :allocation :static)))

(defclass thread-safe-bucket-pool (bucket-pool)
  ((look :initform (bordeaux-threads:make-lock "Bucket Lock"))))

(defmethod pop-bucket :around ((pool thread-safe-bucket-pool))
  (bordeaux-threads:with-recursive-lock-held ((slot-value pool 'look))
    (call-next-method)))

(defmethod push-bucket :around (bucket (pool thread-safe-bucket-pool))
  (bordeaux-threads:with-recursive-lock-held ((slot-value pool 'look))
    (call-next-method)))

(defvar *bucket-pool* (make-instance 'thread-safe-bucket-pool))

(defun alloc-bucket ()
  (pop-bucket *bucket-pool*))

(defun free-bucket (bucket)
  (push-bucket bucket *bucket-pool*))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; brigade
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defclass brigade ()
  ((buckets :initform (make-array 0 :fill-pointer 0 :adjustable t)
            :reader brigade-buckets)
   (bucket-size :initform nil :reader brigade-bucket-size)
   (bucket-index :initform -1)
   (pos-in-bucket :initform 0)))

(defmethod shared-initialize :after ((brigade brigade) slot-names &key)
  (setf (slot-value brigade 'bucket-size)
        (slot-value *bucket-pool* 'bucket-size))
  (brigade-extend brigade))

(defun brigade-extend (brigade)
  (let* ((bucket (alloc-bucket))
         (typespec `(bucket ,(brigade-bucket-size brigade))))
    (assert (typep bucket typespec) (bucket)
            'type-error :datum bucket :expected-type typespec)
    (vector-push-extend bucket
                        (brigade-buckets brigade)))
  (setf (slot-value brigade 'bucket-index)
        (1- (length (brigade-buckets brigade))))
  (setf (slot-value brigade 'pos-in-bucket) 0))

(defun brigade-shift (brigade count)
  (incf (slot-value brigade 'pos-in-bucket) count)
  (when (= (slot-value brigade 'pos-in-bucket)
                          (length (active-bucket brigade)))
    (brigade-extend brigade)))

(defun active-bucket (brigade)
  (values (aref (brigade-buckets brigade)
                (slot-value brigade 'bucket-index))
          (slot-value brigade 'pos-in-bucket)))

(defun brigade-ref (brigade)
  (aref (active-bucket brigade)
        (slot-value brigade 'pos-in-bucket)))

(defun (setf brigade-ref) (newvalue brigade)
  (check-type newvalue ub8)
  (setf (aref (active-bucket brigade)
              (slot-value brigade 'pos-in-bucket))
        newvalue))

(defun brigade-free-buckets (brigade)
  (iter (for bucket in-vector (brigade-buckets brigade))
        (free-bucket bucket))
  (setf (slot-value brigade 'buckets) nil))

(defun brigade-prepare-for-read (brigade)
  (setf (slot-value brigade 'bucket-index) 0
        (slot-value brigade 'pos-in-bucket) 0))


(defun brigade-total-size (brigade)
  (+ (* (brigade-bucket-size brigade)
        (1- (length (brigade-buckets brigade))))
     (slot-value brigade  'pos-in-bucket)))

(defun find-bucket (brigade position)
  (let ((size (brigade-bucket-size brigade)))
    (multiple-value-bind (bucket-index bucket-start) (floor position size)
      (values (aref (brigade-buckets brigade) bucket-index)
              bucket-start
              (if (= bucket-index (slot-value brigade 'bucket-index))
                  (slot-value brigade 'pos-in-bucket)
                  size)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; BSON encode/decode
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmethod decode-byte ((brigade brigade))
  (let ((byte (brigade-ref brigade)))
    (incf (slot-value brigade 'pos-in-bucket))
    (when (= (slot-value brigade 'pos-in-bucket)
             (length (active-bucket brigade)))
      (incf (slot-value brigade 'bucket-index))
      (setf (slot-value brigade 'pos-in-bucket) 0))
    byte))

(defmethod encode-byte (byte (brigade brigade))
  (setf (brigade-ref brigade) byte)
  (incf (slot-value brigade 'pos-in-bucket))
  (when (= (slot-value brigade 'pos-in-bucket)
           (length (active-bucket brigade)))
    (brigade-extend brigade)))

(defmethod bson-target-replace ((brigade brigade) sequence start)
  (iter (for i from start)
        (for j from 0 below (length sequence))
        (multiple-value-bind (index pos) (floor i (length (aref (brigade-buckets brigade) 0)))
          (setf (aref (aref (brigade-buckets brigade)
                            index)
                      pos)
                (elt sequence j)))))

(defmethod encode-protocol-message (message (target (eql :brigade)))
  (encode-protocol-message message
                           (make-instance 'brigade)))

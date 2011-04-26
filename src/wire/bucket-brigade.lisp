;;;; bucket-brigade.lisp
;;;;
;;;; This file is part of the MONGO-CL-DRIVER library, released under Lisp-LGPL.
;;;; See file COPYING for details.
;;;;
;;;; Author: Moskvitin Andrey <archimag@gmail.com>

(in-package #:mongo-cl-driver.wire)

(defconstant +bucket-size+ 4096)

(defparameter *bucket-lock* (bordeaux-threads:make-lock "Bucket Lock"))

(defvar *bucket-pool* nil)

(defun alloc-bucket ()
  (or (bordeaux-threads:with-recursive-lock-held (*bucket-lock*)
        (pop *bucket-pool*))
      (make-array +bucket-size+ :element-type '(unsigned-byte 8))))

(defun free-bucket (bucket)
  (check-type bucket (simple-array (unsigned-byte 8) (4096)))
  (bordeaux-threads:with-recursive-lock-held (*bucket-lock*)
    (push bucket *bucket-pool*)))

(defclass brigade ()
  ((buckets :initform (make-array 0 :fill-pointer 0 :adjustable t)
            :reader brigade-buckets)
   (bucket-index :initform -1)
   (pos-in-bucket :initform 0)))

(defmethod shared-initialize :after ((obj brigade) slot-names &key)
  (brigade-extend obj))

(defun brigade-total-size (brigade)
  (+ (* (1- (length (brigade-buckets brigade))) +bucket-size+)
     (slot-value brigade  'pos-in-bucket)))

(defun brigade-extend (brigade)
  (vector-push-extend (alloc-bucket)
                      (brigade-buckets brigade))
  (incf (slot-value brigade 'bucket-index))
  (setf (slot-value brigade 'pos-in-bucket) 0))

(defun brigade-free-buckets (brigade)
  (iter (for bucket in-vector (brigade-buckets brigade))
        (free-bucket bucket)))

(defun brigade-ref (brigade)
  (aref (aref (brigade-buckets brigade)
              (slot-value brigade 'bucket-index))
        (slot-value brigade 'pos-in-bucket)))

(defun (setf brigade-ref) (newvalue brigade)
  (check-type newvalue (unsigned-byte 8))
  (setf (aref (aref (brigade-buckets brigade)
                    (slot-value brigade 'bucket-index))
              (slot-value brigade 'pos-in-bucket))
        newvalue))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; for support BSON encode/decode
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmethod decode-byte ((brigade brigade))
  (prog1
      (brigade-ref brigade)
    (incf (slot-value brigade 'pos-in-bucket))
    (when (= (slot-value brigade 'pos-in-bucket) +bucket-size+)
      (incf (slot-value brigade 'bucket-index))
      (setf (slot-value brigade 'pos-in-bucket) 0))))

(defmethod encode-byte (byte (brigade brigade))
  (setf (brigade-ref brigade)
        byte)
  (incf (slot-value brigade 'pos-in-bucket))
  (when (= (slot-value brigade 'pos-in-bucket) +bucket-size+)
    (brigade-extend brigade)))

(defmethod bson-target-replace ((brigade brigade) sequence start)
  (iter (for i from start)
        (for j from 0 below (length sequence))
        (multiple-value-bind (index pos) (floor i +bucket-size+)
          (setf (aref (aref (brigade-buckets brigade)
                            index)
                      pos)
                (elt sequence j)))))

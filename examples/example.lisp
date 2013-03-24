(asdf:operate 'asdf:load-op '#:mongo-cl-driver.usocket)
(asdf:operate 'asdf:load-op '#:mongo-cl-driver.cl-async)

(defpackage #:mongo-cl-driver.example
  (:use #:cl #:cl-async-future))

(in-package #:mongo-cl-driver.example)

(mongo-cl-driver.son-sugar:use-son-printer)

(defun usocket-test ()
  (let* ((client (make-instance 'mongo.usocket:mongo-client))
         (db (make-instance 'mongo:database :name "blog" :mongo-client client))
         (stats (mongo:db-stats db))
         (last-err (mongo:last-error db)))
    (format t "~%MONGO-CL-DRIVER.USOCKET:~%")
    (format t "DB stats: ~A~%" stats)
    (format t "Last error: ~A~%" last-err)
    (mongo:mongo-client-close client)))

(defun async-test ()
  (let* ((client (make-instance 'mongo.as:mongo-client))
         (db (make-instance 'mongo:database :name "blog" :mongo-client client)))
    (alet* ((stats (mongo:db-stats db))
            (last-err (mongo:last-error db)))
      (format t "~%MONGO-CL-DRIVER.CL-ASYNC:~%")
      (format t "  DB stats:   ~A~%" stats)
      (format t "  Last error: ~A~%" last-err)
      (mongo:mongo-client-close client)
      (as:exit-event-loop))))

(usocket-test)

(as:start-event-loop 'async-test)

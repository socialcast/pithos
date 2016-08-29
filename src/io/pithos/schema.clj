(ns io.pithos.schema
  "Namespace holding a single action which installs the schema"
  (:require [clojure.tools.logging :refer [info error]]
            [clojure.string        :as string]
            [io.pithos.system      :as system]
            [io.pithos.store       :as store]))

(defn converge-schema-part [desc schema-part]
  (try
    (info (string/join ["converging " desc "..."]))
    (store/converge! schema-part)
  (catch clojure.lang.ExceptionInfo e
    (if (= com.datastax.driver.core.exceptions.AlreadyExistsException (class (get (ex-data e) :exception)))
      (info desc "already exists, continuing.")
      (throw e)))))

(defn converge-schema
  "Loops through all storage layers and calls converge! on them"
  ([system exit?]
     (info "converging all schemas...")
     (try
       (converge-schema-part "bucketstore schema" (system/bucketstore system))
       (doseq [region (system/regions system)
               :let [[region {:keys [metastore storage-classes]}] region]]
         (converge-schema-part (string/join " " ["metastore for" region]) metastore)
         (doseq [[storage-class blobstore] storage-classes]
           (converge-schema-part (string/join " " ["blobstore for region and storage-class" region storage-class]) blobstore)))
       (catch Exception e
         (error e "cannot create schema"))
       (finally
         (when exit? (System/exit 0)))))
  ([system]
     (converge-schema system true)))

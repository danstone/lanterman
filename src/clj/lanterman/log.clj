(ns lanterman.log
  (:require [riverford.durable-ref.core :as dref]
            [lanterman.nodes :as n]
            [lanterman.util :as util])
  (:import (clojure.lang IPersistentCollection Seqable Counted Sequential IReduce IReduceInit IDeref)
           (java.net URI)))

(set! *warn-on-reflection* true)

(deftype PersistentDurableLog [data]
  IPersistentCollection
  (count [this]
    (int (n/sum-length data)))
  (cons [this o]
    (if (instance? PersistentDurableLog o)
      (PersistentDurableLog. (n/append data (.-data ^PersistentDurableLog o)))
      (PersistentDurableLog. (n/append data o))))
  (empty [this]
    (let [{:keys [:log/optimal-slab-bytes
                  :log/root
                  :log/tail]} data
          {:keys [:tree/branching-factor]} root
          {:keys [:tail/max-inline-bytes]} tail]
      (n/empty-log-node
        {:branching-factor branching-factor
         :max-inline-bytes max-inline-bytes
         :log/optimal-slab-bytes optimal-slab-bytes})))
  (equiv [this o]
    (= data data))
  Seqable
  (seq [this]
    (seq (n/message-iterable data)))
  Counted
  Sequential
  IReduce
  (reduce [this f]
    (reduce f (f) (n/message-iterable data)))
  IReduceInit
  (reduce [this f init]
    (reduce f init (n/message-iterable data))))

(defn log
  ([] (log {}))
  ([opts]
   (->PersistentDurableLog (n/empty-log-node opts))))

(deftype LogDurableRef [dref]
  IDeref
  (deref [this]
    (or (when-some [data @dref]
          (->PersistentDurableLog data))
        (log))))

(deftype LogWriter [dref empty storage])

(defn- decide-defaults
  [storage]
  {})

(defn logref
  [uri]
  (let [uri (URI. (str uri))
        scheme (.getScheme uri)]
    (if-not (= scheme "lanterman")
      (throw (IllegalArgumentException. "Not a valid lanterman URI."))
      (->LogDurableRef (dref/reference (str "atomic:" (.getSchemeSpecificPart uri)))))))

(defn log-writer
  ([storage uri]
    (let [defaults (decide-defaults storage)]
      (log-writer storage uri defaults)))
  ([storage uri opts]
   (let [uri (URI. (str uri))
         scheme (.getScheme uri)]
     (if-not (= scheme "lanterman")
       (throw (IllegalArgumentException. "Not a valid lanterman URI."))
       (->LogWriter (dref/reference (str "atomic:" (.getSchemeSpecificPart uri)))
                    (log opts)
                    (let [s storage]
                      (if (map? s)
                        s
                        {:tail s
                         :tree s
                         :slab s})))))))

(defn transact!
  [^LogWriter w f & args]
  (let [dref (.-dref w)
        storage (.-storage w)
        newdata
        (dref/atomic-swap!
          dref
          (fn [x]
            (if (nil? x)
              (recur (.-data ^PersistentDurableLog (.-empty w)))
              (let [ret (apply f (->PersistentDurableLog x) args)]
                (if (instance? PersistentDurableLog ret)
                  (let [data (.-data ^PersistentDurableLog ret)]
                    (n/persist-tree storage data))
                  (throw (IllegalStateException. "Cannot return non-durable log from transact!")))))))]
    (->PersistentDurableLog newdata)))
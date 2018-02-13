(ns lanterman.core
  (:require [lanterman.log :as log]
            [lanterman.nodes :as n]
            [lanterman.util :as util]
            [clojure.set :as set]
            [riverford.durable-ref.core :as dref])
  (:import (java.util UUID)
           (java.lang AutoCloseable)
           (lanterman.log PersistentDurableLog LogDurableRef)))


(def mem {:tree "mem://logstore"
          :slab "mem://logstore"
          :tail "mem://logstore"})

(def tmpfiles
  {:tree "file:///Users/danielstone/tmp/logstmp"
   :slab "file:///Users/danielstone/tmp/logstmp"
   :tail "file:///Users/danielstone/tmp/logstmp"})

(defn empty-log
  ([] (log/log))
  ([opts] (log/log opts)))

(defn logref
  [uri]
  (log/logref uri))

(defn log?
  [x]
  (instance? PersistentDurableLog x))

(defn logref?
  [x]
  (instance? LogDurableRef x))

(defn get-log
  [log]
  (cond
    (log? log) log
    (logref? log) @log
    :else @(logref log)))

(defn log-writer
  ([storage uri]
   (log/log-writer storage uri))
  ([storage uri opts]
   (log/log-writer storage uri opts)))

(defn transact!
  [lw f & args]
  (log/transact! lw #(apply f % args)))

(defn topic
  []
  )

(defn publisher
  "Starts a server that co-ordinates publishes for more perf."
  [broker])

(defn publish!
  "Returns a future that yields data about the published message (e.g offset)."
  [publisher obj])

(defn publish-all!
  "Returns a future that yields data about the published messages. Messages are published atomically."
  [publisher objects])

(defn fetch
  "Returns a seq of messages starting from the offset. Takes a log/uri/logref"
  ([log]
   (fetch log 0))
  ([log offset]
   (n/fetch (get-log log) offset)))

;; topic ref contains

;; cluster meta
;; brokers
;; subscribers

(defonce ^{:arglists '([])} node-id
  (let [d (delay (str (UUID/randomUUID)))]
    (fn []
      @d)))

(defn run-broker
  "Starts a server that acts as a broker, enabling group subscriptions."
  [cluster]

  ;; join req


  )

(defn expose-broker
  "hosts a network broker, so that other nodes can connect directly instead of only locally."
  [broker port])

;; subscriber co-ordination
;; find master broker in broker group
;;

(def topicstate
  {:logrefs #{"atomic:mem://foo/testtopic-1.fressian"
              "atomic:mem://foo/testtopic-2.fressian"
              "atomic:mem://foo/testtopic-3.fressian"
              "atomic:mem://foo/testtopic-4.fressian"}})

(def topicsub
  {:members {"n1" {:connect-time 3242332}}
   :allocations {"" ""}})

(def substate

  ;; granularity of substate, could it be loglevel?

  ;; on each poll
  ;; inc hb-counter
  ;; if still own allocation, return data
  ;; poll-counter does not move for 'session.timeout'
  ;; if a 'dead' member tries to increment a poll, print a warning etc.

  ;; heartbeat(s) are watched by next node in list.
  ;; typical poll rate may be something like 100 ms? round trip swap to dynamo will be on order of 20-30ms.

  ;; e.g 32 consumers which tank dynamo this way?
  ;; just use zk??!
  ;; zookeeper is perfect for root node co-ord too.
  ;; should it be split into fast (zk) /slow (serverless) apis.
  ;; e.g in slow mode, polling is infrequent, prefetch is in but you do not change state very often.
  ;; topic size of 1 with 2 consumer would be ok, I think, 10 consumer would be bad
  ;; maybe print a warning suggesting the use of the server co-ordinated api.

  {:nodes {"n1" {:hb 1
                 :alloc #{"atomic:mem://foo/testtopic-1.fressian"}}}})

{:partitions #{1 2 3 4 5 6 7 8}
 :nodes {"a" {:session-timeout 5000
              :allocations #{1}
              :watched nil
              :watch-time nil}
         "b" {:session-timeout 5000
              :allocations #{2}
              :watched "a"
              :watch-time 432432423}}}

(defn- watch
  [state nodeid]
  (let [nodes (:nodes state)
        sorted-nodes (into (sorted-map) nodes)
        [watched-nodeid watched-node] (or (first (subseq sorted-nodes > nodeid))
                                          (first (remove (comp #{nodeid} key) sorted-nodes)))]
    (if watched-nodeid
      (let [session-timeout (:session-timeout watched-node)
            watched-node-time (:watch-time watched-node)
            watcher (:watcher watched-node)

            kill-watched? (cond
                            (nil? watched-node) false
                            (not= watcher nodeid) false
                            :else (<= (+ watched-node-time session-timeout) (System/currentTimeMillis)))]

        (assoc-in state [:nodes watched-nodeid] (cond
                                                  kill-watched? {:kill? true}
                                                  (not= watcher nodeid) (assoc watched-node
                                                                          :watcher nodeid
                                                                          :watch-time (System/currentTimeMillis))
                                                  :else (assoc watched-node
                                                          :watcher nodeid
                                                          :watch-time (System/currentTimeMillis)))))
      state)))

(defn- clean
  [state]
  (let [nodes (:nodes state)]
    (assoc state :nodes (into {} (for [[id node] nodes
                                       :when (not (:kill? node))]
                                   [id node])))))

(defn- rebalance
  [state nodeid]
  (let [nodes (:nodes state)
        partitions (:partitions state)
        allocated (into #{} (mapcat :allocations) (vals nodes))
        per-node (max 1 (long (Math/ceil (/ (count allocated)
                                            (max 1 (count nodes))))))

        node (get nodes nodeid)
        allocs (:allocations node)]
    (if (> (count allocs) per-node)
      (update-in state [:nodes nodeid :allocations] disj (first allocs))
      state)))

(defn- realloc
  [state]
  (let [nodes (:nodes state)
        partitions (:partitions state)
        allocated (into #{} (mapcat :allocations) (vals nodes))
        to-alloc (set/difference partitions allocated)
        new-nodes (loop [parts to-alloc
                         nodeseq []
                         acc nodes]
                    (cond
                      (empty? parts) acc
                      (empty? nodes) acc
                      (empty? nodeseq) (recur parts (sort-by (comp count :allocations val) (seq acc)) acc)
                      :else
                      (let [[id n] (first nodeseq)]
                        (recur
                          (rest parts)
                          (rest nodeseq)
                          (update-in acc [id :allocations] (fnil conj #{}) (first parts))))))]
    (assoc state :nodes new-nodes)))

(defn touchsub
  [state nodeid]
  (if (contains? (:nodes state) nodeid)
    (-> state
        (watch nodeid)
        clean
        (rebalance nodeid)
        realloc)
    (recur
      (assoc-in state [:nodes nodeid] {:session-timeout 30000
                                       :allocations #{}})
      nodeid)))

(defn- subpoll
  [dref opts]
  (let [nid (node-id)]
    (dref/atomic-swap!
      dref
      #(touchsub % nid)
      opts)))

(defprotocol IBroker
  (-subscribe [this subid topic f]))

(defprotocol ISubscription
  (-commit! [this partition offset]))

(defn- ref-subscribe
  [ref topic f]
  (let [{:keys [partitions]} topic
        stref (volatile!
                (swap! ref (fn [st] (if (empty? (:partitions st))
                                      (assoc st :partitions (set partitions))
                                      st))))
        heartbeat (util/schedule 15000 (fn [] (vreset! stref (subpoll ref {}))))]
    (reify
      ISubscription
      (-commit! [this partition offset]
        )
      AutoCloseable
      (close [this]
        (.close heartbeat)))))

(extend-protocol IBroker
  String
  (-subscribe [this subid topic f]
    (let [{:keys [id]} topic
          ref (dref/reference (doto (str "atomic:" this "/" subid "-" id ".edn")))]
      (ref-subscribe ref topic f))))

;; what should a subscription do?


(defn subscribe
  "Returns a subscription to the topic."
  [broker subid topic f]
  (-subscribe broker subid topic f)

  ;; registry
  ;; heartbeats

  ;; election
  ;; leader rebalance

  )

(defn commit!
  [subscription offset])

(defn transformation
  ;; exactly once delivery bla bla
  [tid xf opts])

(defn transform
  [broker t from to])


;; look at ref
;; first node is leader
;; new leader

(comment

  (let [r (java.util.Random.)]
    (defn- rand-bytes
      [n]
      (let [buf (byte-array n)]
        (.nextBytes r buf)
        buf))))
(ns lanterman.nodes
  (:require [clojure.edn :as edn]
            [clojure.data.fressian :as fress]
            [riverford.durable-ref.core :as dref]
            [riverford.durable-ref.format.fressian])
  (:import (java.nio ByteBuffer)
           (java.util LinkedHashMap)))

(defonce ^:private slab-cache
  (proxy [LinkedHashMap] []
    (removeEldestEntry [eldest]
      (> (.size this) 64))))

(defonce ^:private tree-cache
  (proxy [LinkedHashMap] []
    (removeEldestEntry [eldest]
      (> (.size this) 128))))

(defonce ^:private tail-cache
  (proxy [LinkedHashMap] []
    (removeEldestEntry [eldest]
      (> (.size this) 64))))

(defn- intern-tree-ref
  [dref]
  (locking tree-cache
    (.put tree-cache (str (dref/uri dref)) dref))
  dref)

(defn- intern-tail-ref
  [dref]
  (locking tail-cache
    (.put tail-cache (str (dref/uri dref)) dref))
  dref)

(defn- intern-slab-ref
  [dref]
  (locking slab-cache
    (.put slab-cache (str (dref/uri dref)) dref))
  dref)

(defn- sum-bytes
  [& nodes]
  (transduce (keep :node/bytes) + 0 nodes))

(defn sum-length
  [& nodes]
  (transduce (keep :node/length) + 0 nodes))

(defn- empty-node?
  [node]
  (zero? (sum-length node)))

(defn- buffer?
  [n]
  (= :node.type/buffer (:node/type n)))

(defn- node?
  [x]
  (some? (:node/type x)))

(defn- fressian-bytes
  [x]
  (.array ^ByteBuffer (fress/write x)))

(defn buffer
  [x]
  (cond
    (buffer? x) x

    (bytes? x) {:node/type :node.type/buffer
                :node/bytes (alength ^bytes x)
                :node/length 1

                :buffer/type :bytes
                :buffer/payload x}

    (node? x) (let [bytes (fressian-bytes x)]
                {:node/type :node.type/buffer
                 :node/bytes (alength bytes)
                 :node/length (sum-length x)

                 :buffer/type :node
                 :buffer/payload bytes})

    :else (let [bytes (.getBytes ^String (pr-str x) "UTF-8")]
            {:node/type :node.type/buffer
             :node/bytes (alength bytes)
             :node/length 1
             :buffer/type :edn
             :buffer/payload bytes})))

(defmulti buffer-educt :node/type)

(defmethod buffer-educt :node.type/buffer
  [msg]
  (let [{:keys [:buffer/type
                :buffer/payload]} msg]
    [msg]))

(let [xf (fn [rf]
           (fn
             ([] (rf))
             ([ret] (rf ret))
             ([ret x] (case (:buffer/type x)
                        :node (reduce rf ret (buffer-educt (fress/read (:buffer/payload x))))
                        (rf ret x)))))]
  (defmethod buffer-educt :node.type/tail
    [{:keys [:tail/nodes
             :tail/messages]}]
    (eduction cat [(eduction (mapcat buffer-educt) nodes)
                   (eduction xf messages)])))

(defmethod buffer-educt :node.type/pointer
  [{:keys [:pointer/node]}]
  (buffer-educt node))

(defmulti message-educt :node/type)

(defmethod message-educt :default
  [node]
  (eduction (mapcat message-educt) (buffer-educt node)))

(defmethod message-educt :node.type/buffer
  [msg]
  (let [{:keys [:buffer/type
                :buffer/payload]} msg]
    (case type
      :edn [(edn/read-string (String. ^bytes payload "UTF-8"))]
      :bytes [payload]
      :node (message-educt (fress/read payload)))))

(declare add-node-to-tail)

(defn- add-buffer-to-tail
  [tail buffer]
  (let [{:keys [:tail/nodes
                :tail/messages
                :tail/inline-bytes
                :tail/max-inline-bytes]} tail]
    (cond
      ;; buffer too big, introduce indirection
      (< max-inline-bytes (alength ^bytes (:buffer/payload buffer)))
      (add-node-to-tail tail
                        {:node/type :node.type/slab
                         :node/length (sum-length buffer)
                         :node/bytes (sum-bytes buffer)
                         :slab/buffers [buffer]})

      ;; buffer will not fit in remaining inline bytes, shift tail
      (< max-inline-bytes (+ inline-bytes (alength ^bytes (:buffer/payload buffer))))
      {:node/type :node.type/tail
       :node/bytes (sum-bytes tail buffer)
       :node/length (sum-length tail buffer)

       :tail/inline-bytes (alength ^bytes (:buffer/payload buffer))
       :tail/max-inline-bytes max-inline-bytes
       :tail/nodes [tail]
       :tail/messages [buffer]}

      ;; we have room, can just append buffer
      :else
      {:node/type :node.type/tail
       :node/bytes (+ (sum-bytes tail) (alength ^bytes (:buffer/payload buffer)))
       :node/length (sum-length tail buffer)

       :tail/inline-bytes (+ inline-bytes (alength ^bytes (:buffer/payload buffer)))
       :tail/max-inline-bytes max-inline-bytes
       :tail/nodes nodes
       :tail/messages (conj (vec messages) buffer)})))

(defn- add-node-to-tail
  [tail node]
  (cond
    (empty-node? node) tail
    (buffer? node) (add-buffer-to-tail tail node)
    ;; if we can fit all messages into inline-bytes, do so.
    (< (+ (sum-bytes node) (:tail/inline-bytes tail)) (:tail/max-inline-bytes tail))
    (reduce add-buffer-to-tail tail (buffer-educt node))
    :else
    (let [{:keys [:tail/nodes
                  :tail/messages
                  :tail/inline-bytes
                  :tail/max-inline-bytes]} tail]
      {:node/type :node.type/tail
       :node/bytes (sum-bytes tail node)
       :node/length (sum-length tail node)

       :tail/inline-bytes 0
       :tail/max-inline-bytes max-inline-bytes
       :tail/nodes (if (empty-node? tail)
                     [node]
                     [tail node])
       :tail/messages []})))

(defn- empty-tail
  [max-inline-bytes]
  {:node/type :node.type/tail
   :node/bytes 0
   :node/length 0

   :tail/inline-bytes 0
   :tail/max-inline-bytes max-inline-bytes
   :tail/nodes []
   :tail/messages []})

(defn- add-to-tail
  ([tail x]
   (if (node? x)
     (add-node-to-tail tail x)
     (add-buffer-to-tail tail (buffer x))))
  ([tail x & more]
   (reduce add-to-tail (add-to-tail tail x) more)))

(defn empty-tree
  [branching-factor]
  (assert (< 1 branching-factor) "Branching factor must be greater than 1.")
  {:node/type :node.type/tree
   :node/length 0
   :node/bytes 0

   :tree/offset 0
   :tree/branching-factor branching-factor
   :tree/nodes []})

(defn- ensure-vec [x]
  (if (vector? x) x (vec x)))

(defn- push-slab
  [tree slab]
  (let [{:keys [:tree/nodes
                :tree/branching-factor]} tree
        nodes (ensure-vec nodes)]

    (cond

      (empty? nodes)
      {:node/type :node.type/tree
       :node/length (sum-length slab)
       :node/bytes (sum-bytes slab)


       :tree/branching-factor branching-factor
       :tree/nodes [{:offset 0
                     :bytes (sum-bytes slab)
                     :length (sum-length slab)
                     :nslabs 1
                     :value slab}]}

      (apply = (map :nslabs nodes))
      (if (< (count nodes) branching-factor)
        {:node/type :node.type/tree
         :node/length (sum-length tree slab)
         :node/bytes (sum-bytes tree slab)

         :tree/branching-factor branching-factor
         :tree/nodes (conj nodes
                           {:offset (sum-length tree)
                            :bytes (sum-bytes slab)
                            :length (sum-length slab)
                            :nslabs 1
                            :value slab})}
        (push-slab
          {:node/type :node.type/tree
           :node/length (sum-length tree)
           :node/bytes (sum-bytes tree)

           :tree/branching-factor branching-factor
           :tree/nodes [{:offset 0
                         :bytes (sum-bytes tree)
                         :length (sum-length tree)
                         :nslabs (reduce + (map :nslabs nodes))
                         :value tree}]}
          slab))

      :else

      {:node/type :node.type/tree
       :node/length (sum-length tree slab)
       :node/bytes (sum-bytes tree slab)

       :tree/branching-factor branching-factor
       :tree/nodes (conj (pop nodes)
                         (let [n (peek nodes)
                               child (:value n)
                               ref? (= (:node/type child) :node.type/ref)
                               reftype (:ref/node-type child)
                               child (if (= reftype :node.type/tree)
                                       (dref/value (intern-tree-ref (dref/reference (:ref/uri child))))
                                       child)]
                           (case (:node/type child)
                             :node.type/tree
                             (let [new-tree (push-slab child slab)]
                               {:offset (:offset n)
                                :bytes (sum-bytes new-tree)
                                :length (sum-length new-tree)
                                :nslabs (inc (:nslabs n))
                                :value new-tree})

                             (:node.type/slab :node.type/ref)
                             (let [new-tree (-> (empty-tree branching-factor)
                                                (push-slab child)
                                                (push-slab slab))]
                               {:offset (:offset n)
                                :bytes (sum-bytes new-tree)
                                :length (sum-length new-tree)
                                :nslabs 2
                                :value new-tree}))))})))

(defn empty-log-node
  [opts]
  (let [{:keys [branching-factor
                optimal-slab-bytes
                max-inline-bytes]} opts]
    {:node/type :node.type/log
     :node/bytes 0
     :node/length 0

     :log/root (empty-tree (or branching-factor 2048))
     :log/tail (empty-tail (or max-inline-bytes 4096))
     :log/optimal-slab-bytes (or optimal-slab-bytes (* 512 1024))}))

(defn slabify
  [node]
  (let [buffers (vec (buffer-educt node))]
    {:node/type :node.type/slab
     :node/bytes (sum-bytes node)
     :node/length (sum-length node)
     :slab/buffers buffers}))

(defn- unref
  [node]
  (let [{:keys [:node/type
                :ref/node-type
                :ref/uri]} node]
    (if (= type :node.type/ref)
      (dref/value
        ((case node-type
           :node.type/tree intern-tree-ref
           :node.type/tail intern-tail-ref
           :node.type/slab intern-slab-ref
           identity)
          (dref/reference uri)))
      node)))

(defn add-to-log
  ([log x]
   (if-not (node? x)
     (add-to-log log (buffer x))
     (let [{:keys [:log/tail
                   :log/root
                   :log/optimal-slab-bytes]} log]
       (if (<= optimal-slab-bytes (sum-bytes tail))
         (let [newslab (slabify tail)]
           (add-to-log
             (assoc log
               :log/root (push-slab (unref root) newslab)
               :log/tail (empty-tail (:tail/max-inline-bytes tail)))
             x))
         {:node/type :node.type/log
          :node/bytes (sum-bytes log x)
          :node/length (sum-length log x)

          :log/root root
          :log/tail (add-to-tail tail x)
          :log/optimal-slab-bytes optimal-slab-bytes}))))
  ([log x & more]
   (reduce add-to-log (add-to-log log x) more)))

(defmethod buffer-educt :node.type/log
  [{:keys [:log/root
           :log/tail]}]
  (eduction cat [(buffer-educt root)
                 (buffer-educt tail)]))

(defmethod buffer-educt :node.type/tree
  [{:keys [:tree/nodes]}]
  (eduction (mapcat (comp buffer-educt :value)) nodes))

(defmethod buffer-educt :node.type/slab
  [{:keys [:slab/buffers]}]
  buffers)

(defmethod buffer-educt :node.type/ref
  [{:keys [:ref/uri :ref/node-type]}]
  (buffer-educt (dref/value
                  ((case node-type
                      :node.type/slab intern-slab-ref
                      :node.type/tail intern-tail-ref
                      :node.type/tree intern-tree-ref
                      identity)

                    (dref/reference uri)))))

(defn summarise
  [node]
  (case (:node/type node)

    :node.type/ref {:ref (:ref/uri node)
                    :l (sum-length node)
                    :b (sum-bytes node)}

    :node.type/log {:root (summarise (:log/root node))
                    :tail (summarise (:log/tail node))}

    :node.type/tree (mapv (fn [x]
                            {:nslabs (:nslabs x)
                             :value (summarise (:value x))}) (:tree/nodes node))

    {:l (sum-length node)
     :b (sum-bytes node)}))

(defn persist
  [storage node]
  (let [slab-storage (:slab storage)
        slab-base-uri (if (string? slab-storage)
                        slab-storage)

        tree-storage (:tree storage)
        tree-base-uri (if (string? tree-storage)
                        tree-storage)

        tail-storage (:tail storage)
        tail-base-uri (if (string? tail-storage)
                        tail-storage)
        do-persist (fn [kind obj]
                     (str
                       (dref/uri
                         ((case kind
                            :slab intern-slab-ref
                            :tree intern-tree-ref
                            :tail intern-tail-ref)
                           (dref/persist (case kind
                                           :slab slab-base-uri
                                           :tree tree-base-uri
                                           :tail tail-base-uri)
                                         obj
                                         {:as "fressian"})))))]
    ((fn persist
       ([node] (persist node false))
       ([node log-root?]
        (case (:node/type node)
          :node.type/log (-> node
                             (update :log/root #(future (persist % log-root?)))
                             (update :log/tail #(future (persist % log-root?)))
                             (update :log/root deref)
                             (update :log/tail deref))
          :node.type/tree
          (if (empty? (:tree/nodes node))
            node
            {:node/type :node.type/ref
             :node/bytes (sum-bytes node)
             :node/length (sum-length node)

             :ref/node-type :node.type/tree
             :ref/uri (do-persist
                        :tree
                        (update node :tree/nodes
                                (fn [nodes]
                                  (vec (pmap #(update % :value persist) nodes)))))})

          :node.type/slab {:node/type :node.type/ref
                           :node/bytes (sum-bytes node)
                           :node/length (sum-length node)

                           :ref/node-type :node.type/slab
                           :ref/uri (do-persist :slab node)}

          :node.type/tail (if log-root?
                            (update node :tail/nodes (partial mapv persist))
                            {:node/type :node.type/ref
                             :node/bytes (sum-bytes node)
                             :node/length (sum-length node)

                             :ref/node-type :node.type/tail
                             :ref/uri (do-persist :tail node)})
          node)))
      node
      true)))


(defmulti fetch (fn [node offset] (:node/type node)))

(defmethod fetch :default
  [node offset]
  (drop offset (message-educt node)))

(defmethod fetch :node.type/tree
  [node offset]
  (let [{:keys [:tree/nodes]} node]
    (loop [nodes nodes]
      (if (seq nodes)
        (if (<= (:offset (first nodes)) offset)
          (concat (fetch (:value (first nodes)) (- offset (:offset (first nodes))))
                  (mapcat message-educt (rest nodes)))
          (recur (rest nodes)))
        []))))
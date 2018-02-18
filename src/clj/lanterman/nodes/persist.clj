(ns lanterman.nodes.persist
  "Defines how to serialize/deserialize the multiple node types."
  (:require [riverford.durable-ref.core :as dref])
  (:import (java.nio ByteBuffer HeapByteBuffer)
           (java.io BufferedOutputStream InputStream DataInputStream)))

(declare node-array read-node)

(set! *warn-on-reflection* true)

(def ^:const buffer-overhead
  (+ 1
     ;; byte-count
     8
     ;; len
     8
     4))

(defn buffer-array
  [node]
  (let [{:keys [:node/length
                :node/byte-count
                :buffer/payload
                :buffer/type]} node
        payload (case type
                  :node (node-array (:node-buffer/node node))
                  :nil nil
                  payload)
        totallen (+ buffer-overhead (count payload))
        buf (ByteBuffer/allocate totallen)]
    (.put buf (byte (case type
                      :bytes 0
                      :node 1
                      :string 2
                      :fressian 3
                      :nil 4)))
    (.putLong buf byte-count)
    (.putLong buf length)
    (.putInt buf (count payload))
    (when (some? payload)
      (.put buf ^bytes payload))
    (.array buf)))

(defn read-buffer
  [^ByteBuffer buf]
  (let [buffer-type (case (.get buf)
                      0 :bytes
                      1 :node
                      2 :string
                      3 :fressian
                      4 :nil)
        byte-count (.getLong buf)
        length (.getLong buf)
        payload-len (.getInt buf)]
    (case buffer-type
      :node
      (let [node (read-node buf {})]
        {:node/type :node.type/buffer
         :node/byte-count byte-count
         :node/length length
         :buffer/inline-bytes (+ buffer-overhead payload-len)
         :buffer/type buffer-type
         :node-buffer/node node})
      :nil {:node/type :node.type/buffer
            :node/byte-count byte-count
            :node/length length
            :buffer/inline-bytes (+ buffer-overhead payload-len)
            :buffer/type buffer-type}
      (let [payload (byte-array payload-len)]
        (.get buf payload)
        {:node/type :node.type/buffer
         :node/byte-count byte-count
         :node/length length
         :buffer/inline-bytes  (+ buffer-overhead payload-len)
         :buffer/type buffer-type
         :buffer/payload payload}))))

(def ^:const slab-overhead
  (+
    ;; total len
    4
    ;; byte cont
    8
    ;; length
    8
    ;; buffer count
    4))

(defn slab-array
  [slab]
  (let [{:keys [:slab/buffers
                :node/length
                :node/byte-count]} slab
        buffer-arrays (mapv buffer-array buffers)
        totallen (+ slab-overhead (transduce (map count) + 0 buffer-arrays))
        buf (ByteBuffer/allocate totallen)]
    (.putInt buf totallen)
    (.putLong buf byte-count)
    (.putLong buf length)
    (.putInt buf (count buffers))
    (run! (fn [arr] (.put buf ^bytes arr)) buffer-arrays)
    (.array buf)))

(defn read-slab
  [^ByteBuffer buf opts]
  (when-not (:skip-totallen? opts)
    (.getInt buf))
  (let [byte-count (.getLong buf)
        length (.getLong buf)
        buffer-count (.getInt buf)
        buffers (loop [v (transient [])
                       i (int 0)]
                  (if (< i buffer-count)
                    (recur
                      (conj! v (read-buffer buf))
                      (unchecked-add-int i 1))
                    (persistent! v)))]
    {:node/type :node.type/slab
     :node/byte-count byte-count
     :node/length length
     :slab/empty? (every? (fn [buffer] (= :nil (:buffer/type buffer))) buffers)
     :slab/buffers buffers}))


(defmethod dref/serialize "slab"
  [node _ opts]
  (slab-array node))

(defmethod dref/deserialize "slab"
  [^bytes arr _ opts]
  (let [buf (ByteBuffer/wrap arr)
        slab (read-slab buf {})]
    slab))

(def ^:const tail-overhead
  (+
    ;; totallen
    4
    ;; byte-count
    8
    ;;length
    8
    ;;inline-bytes
    4
    ;;max-inline-bytes
    4
    ;;nodes cnt
    4
    ;;buffers cnt
    4))

(defn tail-array
  [tail]
  (let [{:keys [:node/byte-count
                :node/length
                :tail/root-inline-bytes
                :tail/max-inline-bytes
                :tail/nodes
                :tail/buffers]} tail
        node-arrays (mapv node-array nodes)
        node-array-byte-count (transduce (map count) + 0 node-arrays)
        buffer-arrays (mapv buffer-array buffers)
        buffer-array-byte-count (transduce (map count) + 0 buffer-arrays)
        totallen (+ tail-overhead node-array-byte-count buffer-array-byte-count)
        buf (ByteBuffer/allocate totallen)]
    (.putInt buf totallen)
    (.putLong buf byte-count)
    (.putLong buf length)
    (.putInt buf root-inline-bytes)
    (.putInt buf max-inline-bytes)
    (.putInt buf (count nodes))
    (run! (fn [arr] (.put buf ^bytes arr)) node-arrays)
    (.putInt buf (count buffers))
    (run! (fn [arr] (.put buf ^bytes arr)) buffer-arrays)
    (.array buf)))

(defn read-tail
  [^ByteBuffer buf opts]
  (when-not (:skip-totallen? opts)
    (.getInt buf))
  (let [byte-count (.getLong buf)
        length (.getLong buf)
        inline-bytes (.getInt buf)
        max-inline-bytes (.getInt buf)
        node-count (.getInt buf)
        nodes (loop [v (transient [])
                     i (int 0)]
                (if (< i node-count)
                  (recur
                    (conj! v (read-node buf {}))
                    (unchecked-add-int i 1))
                  (persistent! v)))
        buffer-count (.getInt buf)
        buffers (loop [v (transient [])
                       i (int 0)]
                  (if (< i buffer-count)
                    (recur
                      (conj! v (read-buffer buf))
                      (unchecked-add-int i 1))
                    (persistent! v)))]
    {:node/type :node.type/tail
     :node/byte-count byte-count
     :node/length length
     :tail/root-inline-bytes inline-bytes
     :tail/max-inline-bytes max-inline-bytes
     :tail/nodes nodes
     :tail/buffers buffers}))

(defmethod dref/serialize "tail"
  [node _ opts]
  (tail-array node))

(defmethod dref/deserialize "tail"
  [^bytes arr _ opts]
  (let [buf (ByteBuffer/wrap arr)
        tail (read-tail buf {})]
    tail))

(def ^:const tree-el-overhead
  (+
    ;; offset
    8
    ;; bytes
    8
    ;; len
    8
    ;; nslabs
    8))

(defn tree-el-array
  [tree-el]
  (let [{:keys [:offset
                :bytes
                :length
                :nslabs
                :value]} tree-el
        ^bytes node-array (node-array value)
        totallen (+ tree-el-overhead (alength node-array))
        buf (ByteBuffer/allocate totallen)]
    (.putLong buf offset)
    (.putLong buf bytes)
    (.putLong buf length)
    (.putLong buf nslabs)
    (.put buf node-array)
    (.array buf)))

(defn read-tree-el
  [^ByteBuffer buf]
  (let [offset (.getLong buf)
        byte-count (.getLong buf)
        length (.getLong buf)
        nslabs (.getLong buf)
        value (read-node buf {})]
    {:offset offset
     :bytes byte-count
     :length length
     :nslabs nslabs
     :value value}))

(def ^:const tree-overhead
  (+
    ;; totallen
    4
    ;; bytes
    8
    ;; len
    8
    ;; bf
    4
    ;; node-count
    4))

(defn tree-array
  [tree]
  (let [{:keys [:node/length
                :node/byte-count
                :tree/branching-factor
                :tree/elements]} tree
        element-arrays (mapv tree-el-array elements)
        element-arrays-count (transduce (map count) + 0 element-arrays)
        totallen (+ tree-overhead element-arrays-count)
        buf (ByteBuffer/allocate totallen)]
    (.putInt buf totallen)
    (.putLong buf byte-count)
    (.putLong buf length)
    (.putInt buf branching-factor)
    (.putInt buf (count elements))
    (run! (fn [el] (.put buf ^bytes el)) element-arrays)
    (.array buf)))

(defn read-tree
  [^ByteBuffer buf opts]
  (when-not (:skip-totallen? opts)
    (.getInt buf))
  (let [byte-count (.getLong buf)
        length (.getLong buf)
        bf (.getInt buf)
        elcount (.getInt buf)
        elements (loop [v (transient [])
                        i (int 0)]
                   (if (< i elcount)
                     (recur
                       (conj! v (read-tree-el buf))
                       (unchecked-add-int i 1))
                     (persistent! v)))]
    {:node/type :node.type/tree
     :node/byte-count byte-count
     :node/length length
     :tree/branching-factor bf
     :tree/elements elements}))

(defmethod dref/serialize "tree"
  [node _ opts]
  (tree-array node))

(defmethod dref/deserialize "tree"
  [^bytes arr _ opts]
  (let [buf (ByteBuffer/wrap arr)
        tree (read-tree buf {})]
    tree))

(def ^:const refnode-overhead
  (+ 8 8 8 1 4))

(defn ref-array
  [refnode]
  (let [{:keys [:node/byte-count
                :node/length
                :ref/persist-inst
                :ref/node-type
                :ref/empty?
                :ref/uri]} refnode
        ^bytes uribytes (if empty?
                          (byte-array 0)
                          (.getBytes ^String uri "UTF-8"))
        totallen (+ refnode-overhead (alength uribytes))
        buf (ByteBuffer/allocate totallen)]
    (.putLong buf byte-count)
    (.putLong buf length)
    (.putLong buf persist-inst)
    (.put buf (byte (case node-type
                      :node.type/buffer 0
                      :node.type/slab 1
                      :node.type/tail 2
                      :node.type/tree 3)))
    (.putInt buf (count uribytes))
    (.put buf uribytes)
    (.array buf)))

(defn read-ref
  [^ByteBuffer buf]
  (let [byte-count (.getLong buf)
        length (.getLong buf)
        persist-inst (.getLong buf)
        node-type (case (.get buf)
                    0 :node.type/buffer
                    1 :node.type/slab
                    2 :node.type/tail
                    3 :node.type/tree)
        urilen (.getInt buf)
        uriarr (byte-array urilen)
        empty? (= (count uriarr) 0)]
    (.get buf uriarr)
    (cond->
      {:node/type :node.type/ref
       :node/length length
       :node/byte-count byte-count
       :ref/persist-inst persist-inst
       :ref/node-type node-type}
      empty? (assoc :ref/empty? true)
      (not empty?) (assoc :ref/uri (String. uriarr "UTF-8")))))

(defn add-ref-overhead
  [ref]
  (let [{:keys [:node/byte-count
                :ref/empty?
                :ref/uri]} ref
        newbyte-count (if empty?
                        (+ byte-count refnode-overhead)
                        (+ byte-count refnode-overhead (count (.getBytes ^String uri "UTF-8"))))]
    (assoc ref :node/byte-count newbyte-count
               :node/inline-bytes newbyte-count)))

(def ^:const log-overhead
  (+
    4
    8
    8
    4))

(defn log-array
  [log]
  (let [{:keys [:node/byte-count
                :node/length
                :log/root
                :log/tail
                :log/optimal-slab-bytes]} log
        ^bytes root-array (node-array root)
        ^bytes tail-array (tail-array tail)
        totallen (+ log-overhead (count root-array) (count tail-array))
        buf (ByteBuffer/allocate totallen)]
    (.putInt buf totallen)
    (.putLong buf byte-count)
    (.putLong buf length)
    (.putInt buf optimal-slab-bytes)
    (.put buf root-array)
    (.put buf tail-array)
    (.array buf)))

(defn read-log
  [^ByteBuffer buf opts]
  (when-not (:skip-totallen? opts)
    (.getInt buf))
  (let [byte-count (.getLong buf)
        length (.getLong buf)
        optimal-slab-bytes (.getInt buf)
        root (read-node buf {})
        tail (read-tail buf {})]
    {:node/type :node.type/log
     :node/byte-count byte-count
     :node/length length
     :log/root root
     :log/tail tail
     :log/optimal-slab-bytes optimal-slab-bytes}))

(defmethod dref/serialize "log"
  [node _ opts]
  (log-array node))

(defmethod dref/deserialize "log"
  [^bytes arr _ opts]
  (let [buf (ByteBuffer/wrap arr)
        log (read-log buf {})]
    log))

(def ^:const logplus-overhead
  (+
    4
    8
    8))

(defn logplus-array
  [logplus]
  (let [{:keys [:node/byte-count
                :node/length
                :logplus/message-log
                :logplus/garbage-log]} logplus
        ^bytes message-array (log-array message-log)
        ^bytes garbage-array (garbage-log garbage-log)
        totallen (+ logplus-overhead (count message-array) (count garbage-array))
        buf (ByteBuffer/allocate totallen)]
    (.putInt buf totallen)
    (.putLong buf byte-count)
    (.putLong buf length)
    (.put buf message-array)
    (.put buf garbage-array)))

(defn read-logplus
  [^ByteBuffer buf]
  (.getInt buf)
  (let [byte-count (.getLong buf)
        length (.getLong buf)
        messages (read-log buf {})
        garbage (read-log buf {})]
    {:node/type :node.type/logplus
     :node/byte-count byte-count
     :node/length length
     :logplus/message-log messages
     :logplus/garbage-log garbage}))

(defmethod dref/serialize "logplus"
  [node _ opts]
  (logplus-array node))

(defmethod dref/deserialize "logplus"
  [^bytes arr _ opts]
  (let [buf (ByteBuffer/wrap arr)
        log (read-logplus buf)]
    log))

(def ^:const node-overhead
  (+
    ;; totallen
    4
    ;; type
    1))

(defn node-array
  [node]
  (let [{:keys [:node/type]} node
        arr (case type
              :node.type/buffer (buffer-array node)
              :node.type/slab (slab-array node)
              :node.type/tail (tail-array node)
              :node.type/tree (tree-array node)
              :node.type/ref (ref-array node)
              :node.type/log (log-array node)
              :node.type/logplus (logplus-array node))
        totallen (+ node-overhead (alength ^bytes arr))
        buf (ByteBuffer/allocate totallen)]
    (.putInt buf totallen)
    (.put buf (byte (case type
                      :node.type/buffer 0
                      :node.type/slab 1
                      :node.type/tail 2
                      :node.type/tree 3
                      :node.type/ref 4
                      :node.type/log 5
                      :node.type/logplus 6)))
    (.put buf ^bytes arr)
    (.array buf)))

(defn read-node
  [^ByteBuffer buf opts]
  (when-not (:skip-totallen? opts)
    (.getInt buf))
  (let [node-type (case (.get buf)
                    0 :node.type/buffer
                    1 :node.type/slab
                    2 :node.type/tail
                    3 :node.type/tree
                    4 :node.type/ref
                    5 :node.type/log
                    6 :node.type/logplus)]
    (case node-type
      :node.type/buffer (read-buffer buf)
      :node.type/slab (read-slab buf {})
      :node.type/tail (read-tail buf {})
      :node.type/tree (read-tree buf {})
      :node.type/ref (read-ref buf)
      :node.type/log (read-log buf {})
      :node.type/logplus (read-logplus buf))))

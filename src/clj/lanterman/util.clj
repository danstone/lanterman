(ns lanterman.util
  (:require [clojure.string :as str])
  (:import (java.util.concurrent ScheduledThreadPoolExecutor ScheduledExecutorService TimeUnit ScheduledFuture)
           (java.lang AutoCloseable)))


(defonce ^:private ^ScheduledExecutorService executor-scheduler
  (delay
    (doto (ScheduledThreadPoolExecutor. 2)
      (.setRemoveOnCancelPolicy true))))


(defn- run-scheduled
  [f]
  (try
    (let [thread-ref (volatile! nil)
          lock (Object.)
          fut (future
                (try
                  (vreset! thread-ref (Thread/currentThread))
                  (f)
                  (Thread/interrupted)
                  (catch InterruptedException e
                    ;; swallow

                    )
                  (finally
                    (locking lock
                      (vreset! thread-ref nil))
                    ;; clear any interrupt state
                    (Thread/interrupted))))
          ret (deref fut (* 5 60 1000) ::timed-out)]
      (when (= ::timed-out ret)
        (do
          (locking lock
            (some-> @thread-ref .interrupt))
          (when (= ::timed-out (deref fut (* 5 60 1000) ::timed-out))
            (println "A scheduled task has been interrupted and has refused to stop after 10 minutes, something may
             be wrong.")))))
    (catch Throwable e
      nil)))


(defn schedule-quick
  "Schedules a task to run at a frequency determined by `trigger` if the task
  will take more than 5 minutes, make sure you spawn a new thread and run the task on that, returning the future immediately.

  Allows task execution to overlap. Use `schedule` if this is a problem."
  ([trigger f]
   (schedule-quick trigger f {}))
  ([trigger f opts]
   (let [{:keys [initial-delay]} opts
         ms (cond
              (integer? trigger) (long trigger)
              :else (throw (Exception. "Not a valid trigger")))
         initial-delay (cond
                         (nil? initial-delay) 0
                         (integer? initial-delay) (long initial-delay)
                         :else (throw (Exception. "Not a valid trigger")))
         ^ScheduledFuture fut
         (.scheduleAtFixedRate @executor-scheduler ^Runnable (bound-fn [] (run-scheduled f)) initial-delay ms TimeUnit/MILLISECONDS)]
     (reify AutoCloseable
       (close [this]
         (.cancel fut false))))))

(defn schedule
  "Schedules a task to run at a frequency determined by `trigger`, does not allow task runs to overlap"
  ([trigger f]
   (schedule trigger f {}))
  ([trigger f opts]
   (let [{:keys [timer]} opts
         f (fn []
             (try
               (f)
               (catch InterruptedException e
                 (throw e))
               (catch Throwable e
                 (println "An uncaught error was thrown in scheduled function.")
                 (.printStackTrace e)
                 (throw e))))
         run-agent (agent nil)
         task (schedule-quick
                trigger
                (fn []
                  (send run-agent (fn [fut] (cond (nil? fut) (future (f))
                                                  (realized? fut) (future (f))
                                                  :else fut))))
                opts)]
     (reify AutoCloseable
       (close [this]
         (.close ^AutoCloseable task)
         (send-off run-agent (fn [fut] (cond (nil? fut) nil
                                             (realized? fut) nil
                                             :else (try (future-cancel fut) (catch Throwable e nil)))))
         nil)))))

(defn unschedule
  "Unschedules the task (scheduled via `schedule`)."
  [^AutoCloseable task]
  (.close task))

(defn strip-file-ext
  [s]
  (if-some [idx (str/last-index-of s ".")]
    (subs s 0 idx)
    s))
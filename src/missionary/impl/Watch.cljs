(ns ^:no-doc missionary.impl.Watch
  (:import missionary.Cancelled))

(declare kill transfer)
(deftype Process [reference notifier terminator value]
  IFn
  (-invoke [this] (kill this))
  IDeref
  (-deref [this] (transfer this)))

(defn watch [^Process ps _ _ curr]
  (let [x (.-value ps)]
    (set! (.-value ps) curr)
    (when (identical? x ps)
      ((.-notifier ps)))))

(defn kill [^Process ps]
  (when-some [n (.-notifier ps)]
    (set! (.-notifier ps) nil)
    (remove-watch (.-reference ps) ps)
    (when (identical? (.-value ps) ps)
      (set! (.-value ps) nil) (n))))

(defn transfer [^Process ps]
  (if (nil? (.-notifier ps))
    (do ((.-terminator ps))
        (throw (Cancelled. "Watch cancelled.")))
    (let [x (.-value ps)]
      (set! (.-value ps) ps) x)))

(defn run [r n t]
  (let [ps (->Process r n t @r)]
    (add-watch r ps watch)
    (n) ps))

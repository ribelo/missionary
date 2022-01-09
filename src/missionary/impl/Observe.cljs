(ns ^:no-doc missionary.impl.Observe
  (:import missionary.Cancelled))

(declare kill transfer)

(deftype Process [notifier terminator unsub value]
  IFn
  (-invoke [this] (kill this))
  IDeref
  (-deref [this] (transfer this)))

(defn kill [^Process ps]
  (when-some [n (.-notifier ps)]
    (set! (.-notifier ps) nil)
    (let [x (.-value ps)]
      (set! (.-value ps) nil)
      (when (identical? x ps) (n)))))

(defn transfer [^Process ps]
  (if (nil? (.-notifier ps))
    (do ((.-terminator ps))
        ((.-unsub ps))
        (throw (Cancelled. "Observe cancelled.")))
    (let [x (.-value ps)]
      (set! (.-value ps) ps) x)))

(defn run [s n t]
  (let [ps (->Process n t nil nil)]
    (try (set! (.-value ps) ps)
         (set! (.-unsub ps)
           (s (fn [x]
                (when-not (identical? ps (.-value ps))
                  (throw (js/Error. "Can't process event - consumer is not ready.")))
                (set! (.-value ps) x)
                ((.-notifier ps)))))
         (catch :default e
           (set! (.-unsub ps) #(throw e))
           (kill ps))) ps))


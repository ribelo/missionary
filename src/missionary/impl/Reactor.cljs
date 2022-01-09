(ns missionary.impl.Reactor
  (:import missionary.Cancelled))

(declare event unsubscribe transfer free subscribe kill)

(deftype Failer [t e]
  IFn
  (-invoke [_])
  IDeref
  (-deref [_] (t) (throw e)))

(deftype Subscription
  [notifier terminator subscriber subscribed prev next]
  IFn
  (-invoke [this] (event (.-process subscriber) unsubscribe this))
  IDeref
  (-deref [this] (event (.-process subscriber) transfer this)))

(deftype Publisher
  [process iterator ranks ^number pending ^number children ^boolean live value prev next child sibling active sub]
  IFn
  (-invoke [this] (event process free this))
  (-invoke [this n t] (subscribe this n t)))

(deftype Process
  [result completed failed ^number children ^boolean live ^boolean busy alive active current emitter today tomorrow]
  IFn
  (-invoke [this] (event this kill this)))

(def stale (js-obj))
(def error (js-obj))

(def ^Process current nil)

(defn lt [x y]
  (let [xl (alength x)
        yl (alength y)
        ml (min xl yl)]
    (loop [i 0]
      (if (< i ml)
        (let [xi (aget x i)
              yi (aget y i)]
          (if (== xi yi)
            (recur (inc i))
            (< xi yi)))
        (> xl yl)))))

(defn link [^Publisher x ^Publisher y]
  (if (lt (.-ranks x) (.-ranks y))
    (do (set! (.-sibling y) (.-child x))
        (set! (.-child x) y) x)
    (do (set! (.-sibling x) (.-child y))
        (set! (.-child y) x) y)))

(defn enqueue [root pub]
  (if (nil? root) pub (link pub root)))

(defn dequeue [^Publisher root]
  (loop [heap nil
         prev nil
         head (.-child root)]
    (set! (.-child root) root)
    (if (nil? head)
      (if (nil? prev) heap (if (nil? heap) prev (link heap prev)))
      (let [next (.-sibling head)]
        (set! (.-sibling head) nil)
        (if (nil? prev)
          (recur heap head next)
          (let [head (link prev head)]
            (recur (if (nil? heap) head (link heap head)) nil next)))))))

(defn schedule [^Publisher pub]
  (let [ps (.-process pub)]
    (if (when-some [e (.-emitter ps)] (lt (.-ranks e) (.-ranks pub)))
      (set! (.-today    ps) (enqueue (.-today    ps) pub))
      (set! (.-tomorrow ps) (enqueue (.-tomorrow ps) pub)))))

(defn ack [pub]
  (when (zero? (set! (.-pending pub) (dec (.-pending pub))))
    (set! (.-value pub) nil)
    (when (nil? (.-child pub))
      (schedule pub))))

(defn hook [^Subscription sub]
  (let [pub (.-subscribed sub)]
    (if (nil? (.-prev pub))
      ((.-terminator sub))
      (let [prv (.-sub pub)]
        (set! (.-sub pub) sub)
        (if (nil? prv)
          (->> sub
            (set! (.-prev sub))
            (set! (.-next sub)))
          (let [nxt (.-next prv)]
            (set! (.-next prv) sub)
            (set! (.-prev nxt) sub)
            (set! (.-prev sub) prv)
            (set! (.-next sub) nxt)))))))

(defn sample [^Publisher pub]
  (loop []
    (let [value @(.-iterator pub)]
      (if (nil? (.-child pub))
        (do (set! (.-child pub) pub)
            (recur)) value))))

(defn crash [^Process ps e]
  (when-some [f (.-failed ps)]
    (set! (.-failed ps) nil)
    (set! (.-completed ps) f)
    (set! (.-result ps) e)
    (kill ps)))

(defn cancel [^Publisher pub]
  (when (.-live pub)
    (set! (.-live pub) false)
    ((.-iterator pub))
    (when (identical? stale (.-value pub))
      (try
        (set! (.-pending pub) 1)
        (set! (.-value pub) (sample pub))
        (set! (.-pending pub) 0)
        (catch :default e
          (set! (.-value pub) error)
          (set! (.-pending pub) 0)
          (crash (.-process pub) e))))))

(defn failer [n t e]
  (n) (->Failer t e))

(defn subscribe [^Publisher pub n t]
  (let [ps (.-process pub)
        cur (.-current ps)]
    (if (and (identical? ps current) (some? cur))
      (if (and (not (identical? pub cur)) (lt (.-ranks pub) (.-ranks cur)))
        (let [sub (->Subscription n t cur pub nil nil)]
          (if (identical? (.-active pub) pub)
            (hook sub)
            (do (when-not (zero? (.-pending pub))
                  (set! (.-pending pub) (inc (.-pending pub))))
                (n))) sub)
        (failer n t (js/Error. "Subscription failure : cyclic dependency.")))
      (failer n t (js/Error. "Subscription failure : not in publisher context.")))))

(defn event [^Process ps action target]
  (if (.-busy ps)
    (action target)
    (let [c current]
      (set! current ps)
      (set! (.-busy ps) true)
      (try (action target)
           (finally
             (loop []
               (loop []
                 (when-some [pub (.-active ps)]
                   (set! (.-active ps) (.-active pub))
                   (set! (.-active pub) pub)
                   (ack pub)
                   (recur)))
               (when-some [pub (.-tomorrow ps)]
                 (set! (.-tomorrow ps) nil)
                 (loop [pub pub]
                   (set! (.-today ps) (dequeue pub))
                   (set! (.-emitter ps) pub)
                   (set! (.-current ps) pub)
                   (set! (.-pending pub) 1)
                   (let [t (.-sub pub)]
                     (set! (.-sub pub) nil)
                     (when-not (nil? t)
                       (loop [sub t]
                         (let [sub (.-next sub)]
                           (set! (.-prev sub) nil)
                           (set! (.-pending pub) (inc (.-pending pub)))
                           (when-not (identical? sub t) (recur sub)))))
                     (try (if (identical? (.-active pub) pub)
                            (do (set! (.-value pub) @(.-iterator pub))
                                (set! (.-active pub) (.-active ps))
                                (set! (.-active ps) pub))
                            (do (set! (.-value pub) (if (.-live pub) stale (sample pub)))
                                (set! (.-pending pub) 0)))
                          (catch :default e
                            (set! (.-value pub) error)
                            (set! (.-active pub) nil)
                            (set! (.-pending pub) 0)
                            (crash ps e)))
                     (when-not (nil? t)
                       (loop [nxt (.-next t)]
                         (let [sub nxt nxt (.-next sub)]
                           (set! (.-next sub) nil)
                           (set! (.-current ps) (.-subscriber sub))
                           ((.-notifier sub))
                           (when-not (identical? sub t) (recur nxt))))))
                   (when-some [pub (.-today ps)]
                     (recur pub)))
                 (recur)))
             (set! (.-current ps) nil)
             (set! (.-emitter ps) nil)
             (if (nil? (.-alive ps))
               ((.-completed ps) (.-result ps))
               (set! (.-busy ps) false))
             (set! current c))))))

(defn unsubscribe [^Subscription sub]
  (when-some [pub (.-subscribed sub)]
    (set! (.-subscribed sub) nil)
    (if-some [prv (.-prev sub)]
      (let [ps (.-process pub)
            cur (.-current ps)
            nxt (.-next sub)]
        (set! (.-prev sub) nil)
        (set! (.-next sub) nil)
        (if (identical? prv sub)
          (set! (.-sub pub) nil)
          (do (set! (.-prev nxt) prv)
              (set! (.-next prv) nxt)
              (when (identical? sub (.-sub pub))
                (set! (.-sub pub) prv))))
        (set! (.-current ps) (.-subscriber sub))
        ((.-notifier sub))
        (set! (.-current ps) cur))
      (when-not (zero? (.-pending pub)) (ack sub)))))

(defn transfer [^Subscription s]
  (let [sub (.-subscriber s)
        ps (.-process sub)
        cur (.-current ps)
        value (if-some [pub (.-subscribed s)]
                (let [value (.-value pub)]
                  (if (zero? (.-pending pub))
                    (if (identical? value stale)
                      (try (set! (.-current ps) pub)
                           (set! (.-pending pub) 1)
                           (let [value (sample pub)]
                             (set! (.-value pub) value)
                             (set! (.-pending pub) 0) value)
                           (catch :default e
                             (set! (.-value pub) error)
                             (set! (.-pending pub) 0)
                             (crash ps e) error)) value)
                    (do (ack pub) value))) error)]
    (set! (.-current ps) sub)
    (if (identical? value error)
      (do ((.-terminator s))
          (set! (.-current ps) cur)
          (throw (Cancelled. "Subscription cancelled.")))
      (do (hook s)
          (set! (.-current ps) cur)
          value))))

(defn free [^Publisher pub]
  (let [ps (.-process pub)
        c (.-current ps)]
    (set! (.-current ps) pub)
    (cancel pub)
    (set! (.-current ps) c) nil))

(defn ready [^Publisher pub]
  (set! (.-child pub) nil)
  (when (zero? (.-pending pub))
    (schedule pub) nil))

(defn done [^Publisher pub]
  (let [ps (.-process pub)
        prv (.-prev pub)]
    (set! (.-prev pub) nil)
    (if (identical? pub prv)
      (set! (.-alive ps) nil)
      (let [nxt (.-next pub)]
        (set! (.-next prv) nxt)
        (set! (.-prev nxt) prv)
        (when (identical? (.-alive ps) pub)
          (set! (.-alive ps) prv))))
    (when-some [t (.-sub pub)]
      (set! (.-sub pub) nil)
      (let [cur (.-current ps)]
        (loop [sub (.-next t)]
          (set! (.-current ps) (.-subscriber sub))
          ((.-terminator sub))
          (let [p (.-prev sub)
                n (.-next sub)]
            (set! (.-prev sub) nil)
            (set! (.-next sub) nil)
            (when-not (identical? sub p)
              (set! (.-next p) n)
              (set! (.-prev n) p)
              (recur n))))
        (set! (.-current ps) cur))))
  nil)

(defn boot [^Process ps]
  (try (let [r ((.-result ps))]
         (when-not (nil? (.-failed ps))
           (set! (.-result ps) r)))
       (catch :default e
         (crash ps e))) ps)

(defn kill [^Process ps]
  (when (.-live ps)
    (set! (.-live ps) false)
    (when-some [t (.-alive ps)]
      (let [cur (.-current ps)]
        (loop [pub (.-next t)]
          (cancel pub)
          (when-some [t (.-alive ps)]
            (let [pub (loop [pub pub]
                        (let [pub (.-next pub)]
                          (if (nil? (.-prev pub))
                            (recur pub) pub)))]
              (when-not (identical? pub (.-next t))
                (recur pub)))))
        (set! (.-current ps) cur)))) nil)

(defn run [b s f]
  (let [ps (->Process b s f 0 true false nil nil nil nil nil nil)]
    (event ps boot ps)))

(defn publish [flow continuous]
  (let [ps (doto current (-> nil? (when (throw (js/Error. "Publication failure : not in reactor context.")))))
        par (.-current ps)
        prv (.-alive ps)
        pub (->Publisher
              ps nil
              (if (nil? par)
                (doto (make-array 1) (aset 0 (doto (.-children ps) (->> (inc) (set! (.-children ps))))))
                (let [n (alength (.-ranks par))
                      a (make-array (inc n))]
                  (dotimes [i n] (aset a i (aget (.-ranks par) i)))
                  (doto a (aset n (doto (.-children par) (->> (inc) (set! (.-children par))))))))
              1 0 true nil prv nil nil nil nil nil)]
    (if (nil? prv)
      (->> pub
        (set! (.-prev pub))
        (set! (.-next pub)))
      (let [nxt (.-next prv)]
        (set! (.-next prv) pub)
        (set! (.-prev nxt) pub)
        (set! (.-prev pub) prv)
        (set! (.-next pub) nxt)))
    (set! (.-alive ps) pub)
    (set! (.-current ps) pub)
    (set! (.-child pub) pub)
    (set! (.-iterator pub)
      (flow #(event ps ready pub)
        #(event ps done pub)))
    (when-not (.-live ps) (cancel pub))
    (when continuous (when-not (nil? (.-child pub)) (cancel pub)))
    (if (nil? (.-child pub))
      (try (set! (.-child pub) pub)
           (if continuous
             (do (set! (.-value pub) (if (.-live pub) stale (sample pub)))
                 (set! (.-pending pub) 0))
             (do (set! (.-value pub) @(.-iterator pub))
                 (set! (.-active pub) (.-active ps))
                 (set! (.-active ps) pub)))
           (catch :default e
             (set! (.-value pub) error)
             (set! (.-pending pub) 0)
             (crash ps e)))
      (do (set! (.-active pub) pub)
          (set! (.-pending pub) 0)))
    (set! (.-current ps) par)
    (when continuous (when-not (nil? (.-active pub)) (throw (js/Error. "Undefined continuous flow."))))
    pub))
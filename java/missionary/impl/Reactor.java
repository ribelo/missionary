package missionary.impl;

import java.util.function.Function;
import clojure.lang.AFn;
import clojure.lang.IFn;
import clojure.lang.IDeref;
import missionary.Cancelled;

public interface Reactor {

    final class Failer extends AFn implements IDeref {

        static {
            Util.printDefault(Failer.class);
        }

        IFn terminator;
        Throwable error;

        @Override
        public Object invoke() {
            return null;
        }

        @Override
        public Object deref() {
            terminator.invoke();
            return clojure.lang.Util.sneakyThrow(error);
        }
    }

    final class Subscription extends AFn implements IDeref {

        static {
            Util.printDefault(Subscription.class);
        }

        IFn notifier;
        IFn terminator;
        Publisher subscriber;
        Publisher subscribed;
        Subscription prev;
        Subscription next;

        @Override
        public Object invoke() {
            return event(subscriber.process, unsubscribe, this);
        }

        @Override
        public Object deref() {
            return event(subscriber.process, transfer, this);
        }
    }

    final class Publisher extends AFn {

        static {
            Util.printDefault(Publisher.class);
        }

        Process process;
        Object iterator;
        int[] ranks;
        int pending;
        int children;
        boolean live;
        Object value;
        Publisher prev;
        Publisher next;
        Publisher child;
        Publisher sibling;
        Publisher active;
        Subscription sub;

        @Override
        public Object invoke() {
            return event(process, free, this);
        }

        @Override
        public Object invoke(Object n, Object t) {
            return subscribe(this, (IFn) n, (IFn) t);
        }
    }

    final class Process extends AFn {

        static {
            Util.printDefault(Process.class);
        }

        Object result;
        IFn completed;
        IFn failed;
        int children;
        boolean live;
        boolean busy;
        Publisher alive;
        Publisher active;
        Publisher current;
        Publisher emitter;
        Publisher today;
        Publisher tomorrow;

        @Override
        public Object invoke() {
            return event(this, kill, this);
        }
    }

    Object stale = new Object();
    Object error = new Object();

    ThreadLocal<Process> current = new ThreadLocal<>();

    static boolean lt(int[] x, int[] y) {
        int xl = x.length;
        int yl = y.length;
        int ml = Math.min(xl, yl);
        for(int i = 0; i < ml; i++) {
            int xi = x[i];
            int yi = y[i];
            if (xi != yi) return xi < yi;
        }
        return xl > yl;
    }

    static Publisher link(Publisher x, Publisher y) {
        if (lt(x.ranks, y.ranks)) {
            y.sibling = x.child;
            x.child = y;
            return x;
        } else {
            x.sibling = y.child;
            y.child = x;
            return y;
        }
    }

    static Publisher enqueue(Publisher root, Publisher pub) {
        return root == null ? pub : link(pub, root);
    }

    static Publisher dequeue(Publisher root) {
        Publisher heap = null;
        Publisher prev = null;
        Publisher head = root.child;
        root.child = root;
        while (head != null) {
            Publisher next = head.sibling;
            head.sibling = null;
            if (prev == null) prev = head;
            else {
                head = link(prev, head);
                heap = heap == null ? head : link(heap, head);
                prev = null;
            }
            head = next;
        }
        return prev == null ? heap : heap == null ? prev : link(heap, prev);
    }

    static void schedule(Publisher pub) {
        Process ps = pub.process;
        Publisher e = ps.emitter;
        if (e != null && lt(e.ranks, pub.ranks))
            ps.today = enqueue(ps.today, pub);
        else ps.tomorrow = enqueue(ps.tomorrow, pub);
    }

    static void ack(Publisher pub) {
        if (--pub.pending == 0) {
            pub.value = null;
            if (pub.child == null) schedule(pub);
        }
    }

    static void hook(Subscription sub) {
        Publisher pub = sub.subscribed;
        if (pub.prev == null) sub.terminator.invoke(); else {
            Subscription prv = pub.sub;
            pub.sub = sub;
            if (prv == null) sub.prev = sub.next = sub; else {
                Subscription nxt = prv.next;
                nxt.prev = prv.next = sub;
                sub.prev = prv;
                sub.next = nxt;
            }
        }
    }

    static Object sample(Publisher pub) {
        Object value;
        for(;;) {
            value = ((IDeref) pub.iterator).deref();
            if (pub.child == null) pub.child = pub; else break;
        }
        return value;
    }

    static void crash(Process ps, Throwable e) {
        IFn f = ps.failed;
        if (f != null) {
            ps.failed = null;
            ps.completed = f;
            ps.result = e;
            kill.apply(ps);
        }
    }

    static void cancel(Publisher pub) {
        if (pub.live) {
            pub.live = false;
            ((IFn) pub.iterator).invoke();
            if (pub.value == stale) try {
                pub.pending = 1;
                pub.value = sample(pub);
                pub.pending = 0;
            } catch (Throwable e) {
                pub.value = error;
                pub.pending = 0;
                crash(pub.process, e);
            }
        }
    }

    static Object failer(IFn n, IFn t, Throwable e) {
        n.invoke();
        Failer f = new Failer();
        f.terminator = t;
        f.error = e;
        return f;
    }

    static Object subscribe(Publisher pub, IFn n, IFn t) {
        Process ps = pub.process;
        Publisher cur = ps.current;
        if (ps != current.get() || cur == null)
            return failer(n, t, new Error("Subscription failure : not in publisher context."));
        if (pub == cur || lt(cur.ranks, pub.ranks))
            return failer(n, t, new Error("Subscription failure : cyclic dependency."));
        Subscription sub = new Subscription();
        sub.notifier = n;
        sub.terminator = t;
        sub.subscriber = cur;
        sub.subscribed = pub;
        if (pub == pub.active) hook(sub); else {
            if (pub.pending == 0) {} else pub.pending++;
            n.invoke();
        }
        return sub;
    }

    static <T> Object event(Process ps, Function<T, Object> action, T target) {
        synchronized (ps) {
            if (ps.busy) return action.apply(target); else {
                Process c = current.get();
                current.set(ps);
                ps.busy = true;
                try {
                    return action.apply(target);
                } finally {
                    Publisher pub;
                    for (;;) {
                        while ((pub = ps.active) != null) {
                            ps.active = pub.active;
                            pub.active = pub;
                            ack(pub);
                        }
                        pub = ps.tomorrow;
                        ps.tomorrow = null;
                        if (pub == null) break;
                        else do {
                            ps.today = dequeue(pub);
                            ps.emitter = ps.current = pub;
                            pub.pending = 1;
                            Subscription t = pub.sub;
                            pub.sub = null;
                            if (t != null) {
                                Subscription sub = t;
                                do {
                                    sub = sub.next;
                                    sub.prev = null;
                                    pub.pending++;
                                } while (sub != t);
                            }
                            try {
                                if (pub.active == pub) {
                                    pub.value = ((IDeref) pub.iterator).deref();
                                    pub.active = ps.active;
                                    ps.active = pub;
                                } else {
                                    pub.value = pub.live ? stale : sample(pub);
                                    pub.pending = 0;
                                }
                            } catch (Throwable e) {
                                pub.value = error;
                                pub.active = null;
                                pub.pending = 0;
                                crash(ps, e);
                            }
                            if (t != null) {
                                Subscription nxt = t.next;
                                Subscription sub;
                                do {
                                    sub = nxt;
                                    nxt = sub.next;
                                    sub.next = null;
                                    ps.current = sub.subscriber;
                                    sub.notifier.invoke();
                                } while (sub != t);
                            }
                        } while ((pub = ps.today) != null);
                    }
                    ps.current = ps.emitter = null;
                    if (ps.alive == null) ps.completed.invoke(ps.result);
                    else ps.busy = false;
                    current.set(c);
                }
            }
        }
    }

    Function<Subscription, Object> unsubscribe = (sub) -> {
        Publisher pub = sub.subscribed;
        if (pub != null) {
            sub.subscribed = null;
            Subscription prv = sub.prev;
            if (prv == null) if (pub.pending == 0) {} else ack(pub); else {
                Process ps = pub.process;
                Publisher cur = ps.current;
                Subscription nxt = sub.next;
                sub.prev = sub.next = null;
                if (prv == sub) pub.sub = null; else {
                    prv.next = nxt;
                    nxt.prev = prv;
                    if (pub.sub == sub) pub.sub = prv;
                }
                ps.current = sub.subscriber;
                sub.notifier.invoke();
                ps.current = cur;
            }
        }
        return null;
    };

    Function<Subscription, Object> transfer = (s) -> {
        Publisher pub = s.subscribed;
        Publisher sub = s.subscriber;
        Process ps = sub.process;
        Publisher cur = ps.current;
        Object value;
        if (pub != null) {
            value = pub.value;
            if (pub.pending == 0) {
                if (value == stale) try {
                    ps.current = pub;
                    pub.pending = 1;
                    value = pub.value = sample(pub);
                    pub.pending = 0;
                } catch (Throwable e) {
                    value = pub.value = error;
                    pub.pending = 0;
                    crash(ps, e);
                }
            } else ack(pub);
        } else value = error;
        ps.current = sub;
        if (value == error) {
            s.terminator.invoke();
            ps.current = cur;
            return clojure.lang.Util.sneakyThrow(new Cancelled("Subscription cancelled."));
        } else {
            hook(s);
            ps.current = cur;
            return value;
        }
    };

    Function<Publisher, Object> free = (pub) -> {
        Process ps = pub.process;
        Publisher c = ps.current;
        ps.current = pub;
        cancel(pub);
        ps.current = c;
        return null;
    };

    Function<Publisher, Object> ready = (pub) -> {
        pub.child = null;
        if (pub.pending == 0) schedule(pub);
        return null;
    };

    Function<Publisher, Object> done = (pub) -> {
        Process ps = pub.process;
        Publisher prv = pub.prev;
        pub.prev = null;
        if (pub == prv) ps.alive = null; else {
            Publisher nxt = pub.next;
            nxt.prev = prv;
            prv.next = nxt;
            if (ps.alive == pub) ps.alive = prv;
        }
        Subscription t = pub.sub;
        if (t != null) {
            pub.sub = null;
            Publisher cur = ps.current;
            Subscription sub = t.next;
            for(;;) {
                ps.current = sub.subscriber;
                sub.terminator.invoke();
                Subscription p = sub.prev;
                Subscription n = sub.next;
                sub.prev = sub.next = null;
                if (sub == p) break;
                p.next = n;
                n.prev = p;
                sub = n;
            }
            ps.current = cur;
        }
        return null;
    };

    Function<Process, Object> boot = (ps) -> {
        try {
            Object r = ((IFn) ps.result).invoke();
            if (ps.failed != null) ps.result = r;
        } catch (Throwable e) {
            crash(ps, e);
        }
        return ps;
    };

    Function<Process, Object> kill = (ps) -> {
        if (ps.live) {
            ps.live = false;
            Publisher t = ps.alive;
            if (t != null) {
                Publisher cur = ps.current;
                Publisher pub = t.next;
                for(;;) {
                    cancel(pub);
                    t = ps.alive;
                    if (t == null) break;
                    do pub = pub.next; while (pub.prev == null);
                    if (pub == t.next) break;
                }
                ps.current = cur;
            }
        }
        return null;
    };

    static Object run(IFn init, IFn success, IFn failure) {
        Process ps = new Process();
        ps.result = init;
        ps.completed = success;
        ps.failed = failure;
        ps.live = true;
        return event(ps, boot, ps);
    }

    static Object publish(IFn flow, boolean continuous) {
        Process ps = current.get();
        if (ps == null) throw new Error("Publication failure : not in reactor context.");
        Publisher pub = new Publisher();
        Publisher par = ps.current;
        int[] ranks;
        if (par == null) ranks = new int[] {ps.children++};
        else {
            int size = par.ranks.length;
            ranks = new int[size + 1];
            System.arraycopy(par.ranks, 0, ranks, 0, size);
            ranks[size] = par.children++;
        }
        pub.process = ps;
        pub.ranks = ranks;
        pub.pending = 1;
        pub.live = true;
        Publisher prv = ps.alive;
        if (prv == null) pub.prev = pub.next = pub; else {
            Publisher nxt = prv.next;
            prv.next = nxt.prev = pub;
            pub.prev = prv;
            pub.next = nxt;
        }
        ps.alive = pub;
        ps.current = pub;
        pub.child = pub;
        pub.iterator = flow.invoke(new AFn() {
            @Override
            public Object invoke() {
                return event(ps, ready, pub);
            }
        }, new AFn() {
            @Override
            public Object invoke() {
                return event(ps, done, pub);
            }
        });
        if (!ps.live) cancel(pub);
        if (continuous) if (pub.child != null) cancel(pub);
        if (pub.child == null) try {
            pub.child = pub;
            if (continuous) {
                pub.value = pub.live ? stale : sample(pub);
                pub.pending = 0;
            } else {
                pub.value = ((IDeref) pub.iterator).deref();
                pub.active = ps.active;
                ps.active = pub;
            }
        } catch (Throwable e) {
            pub.value = error;
            pub.pending = 0;
            crash(ps, e);
        } else {
            pub.active = pub;
            pub.pending = 0;
        }
        ps.current = par;
        if (continuous) if (pub.active != null) throw new Error("Undefined continuous flow.");
        return pub;
    }

}
package missionary.impl;

import clojure.lang.*;
import missionary.Cancelled;

public interface Observe {

    final class Process extends AFn implements IDeref {

        IFn notifier;
        IFn terminator;
        Object unsub;
        Object value;

        @Override
        public synchronized Object invoke() {
            kill(this);
            return null;
        }

        @Override
        public synchronized Object deref() {
            return transfer(this);
        }
    }

    static void kill(Process ps) {
        IFn n = ps.notifier;
        if (n != null) {
            ps.notifier = null;
            Object x = ps.value;
            ps.value = null;
            if (x == ps) n.invoke();
        }
    }

    static Object transfer(Process ps) {
        if (ps.notifier == null) {
            ps.terminator.invoke();
            ((IFn) ps.unsub).invoke();
            return clojure.lang.Util.sneakyThrow(new Cancelled("Observe cancelled."));
        } else {
            Object x = ps.value;
            ps.value = ps;
            return x;
        }
    }

    static Object run(IFn sub, IFn n, IFn t) {
        Process ps = new Process();
        synchronized (ps) {
            try {
                ps.notifier = n;
                ps.terminator = t;
                ps.value = ps;
                ps.unsub = sub.invoke(new AFn() {
                    @Override
                    public Object invoke(Object x) {
                        synchronized (ps) {
                            if (ps.value != ps) throw new Error("Can't process event - consumer is not ready.");
                            ps.value = x;
                            return ps.notifier.invoke();
                        }
                    }
                });
            } catch (Throwable e) {
                ps.unsub = new AFn() {
                    @Override
                    public Object invoke() {
                        return clojure.lang.Util.sneakyThrow(e);
                    }
                };
                kill(ps);
            }
            return ps;
        }
    }
}

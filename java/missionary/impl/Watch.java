package missionary.impl;

import clojure.lang.*;
import missionary.Cancelled;

public interface Watch {

    final class Process extends AFn implements IDeref {
        IFn notifier;
        IFn terminator;
        IRef reference;
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

    IFn watch = new AFn() {
        @Override
        public Object invoke(Object key, Object ref, Object prev, Object curr) {
            Process ps = (Process) key;
            synchronized (ps) {
                Object x = ps.value;
                ps.value = curr;
                return x == ps ? ps.notifier.invoke() : null;
            }
        }
    };

    static void kill(Process ps) {
        IFn n = ps.notifier;
        if (n != null) {
            ps.notifier = null;
            ps.reference.removeWatch(ps);
            Object x = ps.value;
            if (x == ps) {
                ps.value = null;
                n.invoke();
            }
        }
    }

    static Object transfer(Process ps) {
        if (ps.notifier == null) {
            ps.terminator.invoke();
            return clojure.lang.Util.sneakyThrow(new Cancelled("Watch cancelled."));
        } else {
            Object x = ps.value;
            ps.value = ps;
            return x;
        }
    }

    static Object run(IRef r, IFn n, IFn t) {
        Process ps = new Process();
        ps.notifier = n;
        ps.terminator = t;
        ps.reference = r;
        ps.value = r.deref();
        r.addWatch(ps, watch);
        n.invoke();
        return ps;
    }

}

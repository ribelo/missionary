package missionary.impl;

import clojure.lang.AFn;
import clojure.lang.IDeref;
import clojure.lang.IFn;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public interface Continuous {

    AtomicReferenceFieldUpdater<Choice, Choice> CHOICE_CHILD =
            AtomicReferenceFieldUpdater.newUpdater(Choice.class, Choice.class, "child");

    Throwable UOE = new UnsupportedOperationException();
    Throwable ISE = new IllegalStateException("Flow is not ready.");
    IFn RUN = new AFn() {
        @Override
        public Object invoke(Object c, Object p) {
            return (((Process) p).coroutine = (IFn) c).invoke();
        }
    };

    static Process process(IFn c, IFn n, IFn t) {
        Process p = new Process();
        p.coroutine = c;
        p.notifier = n;
        p.terminator = t;
        n.invoke();
        return p;
    }

    final class Choice {
        Choice parent;
        Object iterator;
        IFn backtrack;
        volatile Choice child;  // self-reference if ready
    }

    final class Process extends AFn implements IDeref, Fiber {

        Choice choice;
        IFn coroutine;
        IFn notifier;
        IFn terminator;

        @Override
        public Object invoke() {
            // TODO cancel
            return null;
        }

        @Override
        public Object deref() {
            Fiber prev = Fiber.CURRENT.get();
            Fiber.CURRENT.set(this);
            Object x;
            if (choice == null) x = coroutine.invoke();
            else {
                x = Fiber.CURRENT;
                while (choice.parent != null && choice.parent == choice.parent.child)
                    choice = choice.parent;
            }
            while (x == Fiber.CURRENT)
                x = choice.backtrack.invoke(RUN, this);
            Fiber.CURRENT.set(prev);
            return x; // TODO exceptions
        }

        @Override
        public Object poll() {
            // TODO
            return null;
        }

        @Override
        public Object task(IFn t) {
            return clojure.lang.Util.sneakyThrow(UOE);
        }

        @Override
        public Object flowConcat(IFn f) {
            return clojure.lang.Util.sneakyThrow(UOE);
        }

        @Override
        public Object flowSwitch(IFn f) {
            Choice c = new Choice();
            c.iterator = f.invoke(new AFn() {
                @Override
                public Object invoke() {
                    Choice choice = c;
                    for(;;) {
                        Choice child = choice.child;
                        if (choice == child) return null;
                        if (CHOICE_CHILD.compareAndSet(choice, child, choice)) {
                            choice = child;
                            if (child == null) {
                                if (c.backtrack == null) {
                                    c.backtrack = coroutine;
                                    return null;
                                } else return notifier.invoke();
                            }
                        }
                    }
                }
            }, new AFn() {
                @Override
                public Object invoke() {
                    // TODO terminate
                    return null;
                }
            });
            if (choice != null) {
                c.parent = choice;
                if(!CHOICE_CHILD.compareAndSet(choice, null, c))
                    throw new IllegalStateException();   // TODO check if already notified
            }
            choice = c;
            return c.backtrack == null ? clojure.lang.Util.sneakyThrow(ISE) : Fiber.CURRENT;
        }

        @Override
        public Object flowGather(IFn f) {
            return clojure.lang.Util.sneakyThrow(UOE);
        }

        @Override
        public Object unpark() {
            if(!CHOICE_CHILD.compareAndSet(choice, choice, null))
                throw new IllegalStateException();
            return ((IDeref) choice.iterator).deref();
        }
    }

}
package org.zygon.compute;

import java.util.function.Supplier;

/**
 *
 * @author zygon
 */
public class ComputeBase extends Thread {

    private final Supplier<Boolean> isDoneFn;

    public ComputeBase(Supplier<Boolean> isDoneFn) {
        setName(getThreadName());
        super.setDaemon(true);
        this.isDoneFn = isDoneFn;
    }

    public boolean isShutdown() {
        return isDoneFn.get();
    }

    protected final void log(String msg) {
        System.out.println(getThreadName() + ": " + msg);
    }

    private String getThreadName() {
        String clsName = getClass().getSimpleName();
        String threadName = Thread.currentThread().getName();

        return clsName + "-" + threadName;
    }
}

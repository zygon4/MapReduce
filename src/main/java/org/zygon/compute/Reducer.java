package org.zygon.compute;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 *
 * @author zygon
 */
public class Reducer<I> extends ComputeBase {

    private final BlockingQueue<I> input;
    private final BiFunction<I, I, I> reduceFn;
    private final BlockingQueue<I> output;

    // reduceFn should be able to handle null as an input
    public Reducer(BlockingQueue<I> input, Supplier<Boolean> isDoneFn,
            BiFunction<I, I, I> reduceFn, BlockingQueue<I> output) {
        super(isDoneFn);
        this.input = Objects.requireNonNull(input);
        this.reduceFn = Objects.requireNonNull(reduceFn);
        this.output = Objects.requireNonNull(output);
    }

    @Override
    public void run() {

        while (input.isEmpty() && !isShutdown()) {
            log("Waiting for data.");
            // wait a second for data to start flowing..
            try {
                Thread.sleep(500);
            } catch (InterruptedException intr) {
                // TODO: exit
                intr.printStackTrace(System.err);
            }
        }

        while (!input.isEmpty() || !isShutdown()) {

            I element1 = null;
            I element2 = null;

            try {
                element1 = blockOnTake();
                element2 = blockOnTake();
            } catch (InterruptedException intr) {
                // This will probably drop data on the ground
                intr.printStackTrace(System.err);
                break;
            }

            if (element1 != null || element2 != null) {
                // garbage logging mech
//                log("reducing element 1: " + element1 + ", element 2: " + element2);

                I result = reduceFn.apply(element1, element2);

//                log("reducing result: " + result);
                if (element1 != null && element2 != null) {
                    try {
                        input.put(result);
                    } catch (InterruptedException intr) {
                        // This will probably drop data on the ground
                        intr.printStackTrace(System.err);
                    }
                } else {
                    try {
                        output.put(result);
                    } catch (InterruptedException intr) {
                        // This will probably drop data on the ground
                        intr.printStackTrace(System.err);
                    }
                }
            }
        }

        if (!input.isEmpty()) {
            log("DONE prematurely, elements left to process!");
        } else {
            log("DONE");
        }
    }

    private I blockOnTake() throws InterruptedException {
        return input.poll(2, TimeUnit.SECONDS);
    }
}

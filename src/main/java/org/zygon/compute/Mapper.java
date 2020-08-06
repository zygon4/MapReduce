package org.zygon.compute;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 *
 * @author zygon
 */
public class Mapper<I, O> extends ComputeBase {

    private final BlockingQueue<I> input;
    private final Function<I, O> mapFn;
    private final BlockingQueue<O> output;

    public Mapper(BlockingQueue<I> input, Supplier<Boolean> isDoneFn,
            Function<I, O> mapFn, BlockingQueue<O> output) {
        super(isDoneFn);
        this.input = Objects.requireNonNull(input);
        this.mapFn = Objects.requireNonNull(mapFn);
        this.output = Objects.requireNonNull(output);
    }

    @Override
    public void run() {

        while (!isShutdown()) {
            I element = null;
            try {
                element = input.take();
            } catch (InterruptedException intr) {
                // This will probably drop data on the ground
                intr.printStackTrace(System.err);
            }

            // garbage logging mech
//            log("mapping element " + element);
            O reduceMe = mapFn.apply(element);

//            log("mapping result " + reduceMe);
            try {
                output.put(reduceMe);
            } catch (InterruptedException intr) {
                // This will probably drop data on the ground
                intr.printStackTrace(System.err);
            }
        }

        log("DONE");
    }
}

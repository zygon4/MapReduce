package org.zygon.compute;

import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 *
 * @author zygon
 */
public class MapReduce<M, R> {

    private static final int MAPPER_COUNT = 4;
    private static final int REDUCER_COUNT = 3;

    private final ExecutorService executorService = Executors.newFixedThreadPool(8);
    private final Function<M, R> mapperFn;
    private final BinaryOperator<R> reducerFn;

    public MapReduce(Function<M, R> mapperFn, BinaryOperator<R> reducerFn) {
        this.mapperFn = mapperFn;
        this.reducerFn = reducerFn;
    }

    public R processResult(BlockingQueue<M> inputQueue, Supplier<Boolean> isDoneFn) {

        BlockingQueue<R> result = new ArrayBlockingQueue<>(REDUCER_COUNT);

        try {
            CompletionService<Reducer<R>> reducerService
                    = new ExecutorCompletionService(executorService);

            // wiring the queues is weird - maybe just have the reducer keep it's own
            // internal temp queue for processing "pull off either queue - either the
            // primary one or the reduced one"
            // TBD: this queue really shouldn't block, it can cause deadlock
            BlockingQueue<R> mapping = new ArrayBlockingQueue<>(20000);

            final AtomicBoolean mappersDone = new AtomicBoolean(false);
            Supplier<Boolean> reducerIsDone = () -> {
                return isDoneFn.get() && mappersDone.get();
            };

            // start reducers
            for (int i = 0; i < REDUCER_COUNT; i++) {
                Reducer<R> reducer = new Reducer<>(mapping, reducerIsDone, reducerFn, result);
                reducerService.submit(reducer, reducer);
            }

            CompletionService<Mapper<M, R>> mappingService
                    = new ExecutorCompletionService(executorService);

            // start mappers
            for (int i = 0; i < MAPPER_COUNT; i++) {
                Mapper<M, R> mapper = new Mapper<>(inputQueue, isDoneFn, mapperFn, mapping);

                // same object OK?
                mappingService.submit(mapper, mapper);
            }

            // wait for mappers to be done
            for (int i = 0; i < MAPPER_COUNT; i++) {
                Future<Mapper<M, R>> mapperFuture = mappingService.take();
                try {
                    mapperFuture.get();
                } catch (ExecutionException ee) {
                    // dropped ex
                    ee.printStackTrace(System.err);
                }
            }

            // set our reducers to be done
            mappersDone.set(true);

            // wait for reducers to be done
            for (int i = 0; i < REDUCER_COUNT; i++) {
                Future<Reducer<R>> reducerFuture = reducerService.take();
                try {
                    reducerFuture.get();
                } catch (ExecutionException ee) {
                    // dropped ex
                    ee.printStackTrace(System.err);
                }
            }

            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.SECONDS);

            // Perform a final serial reduction
            Iterable<R> iterable = () -> result.iterator();
            Stream<R> targetStream = StreamSupport.stream(iterable.spliterator(), false);
            Optional<R> reduced = targetStream.reduce(reducerFn);

            // or else throw?
            return reduced.orElse(null);
        } catch (InterruptedException intr) {
            intr.printStackTrace(System.err);
            return null;
        } finally {
            if (!executorService.isTerminated()) {
                executorService.shutdownNow();
            }
        }
    }
}

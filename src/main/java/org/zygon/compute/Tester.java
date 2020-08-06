/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.zygon.compute;

import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 *
 * @author zygon
 */
public class Tester {

    /**
     * @param args the command line arguments
     * @throws java.lang.InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {
        MapReduce<String, Integer> processWordCount = new MapReduce<>(
                (String t) -> {
                    String[] split = t.split("\\w+");
                    return split == null ? 0 : split.length;
                },
                (Integer i1, Integer i2) -> {
                    if (i1 == null && i2 == null) {
                        return null;
                    }
                    if (i1 == null && i2 != null) {
                        return i2;
                    }
                    if (i1 != null && i2 == null) {
                        return i1;
                    }
                    return i1 + i2;
                }
        );

        BlockingQueue<String> input = new ArrayBlockingQueue<>(10000);

        Random rand = new Random();

        int totalWordCount = 0;

        for (int i = 0; i < 9000; i++) {
            int wordCount = rand.nextInt(256);

            String randomSentence = rand.ints(wordCount)
                    .mapToObj((int w) -> String.valueOf(w))
                    .collect(Collectors.joining(" "));

            totalWordCount += wordCount;

            input.put(randomSentence);
        }

        long startNs = System.nanoTime();

        int result = processWordCount.processResult(input, input::isEmpty);

        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);

        // TODO: test
        System.out.println("wordCount: " + result + ", expected count " + totalWordCount + ", took " + durationMs + "ms");
    }

}

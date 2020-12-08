package org.bist.statistics;

import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/*
 * Stores all latency results of current test
 * GC is not desired during the tests so an array of primitive long holder class is pre-created and reused all the time
 *
 * getSortedResults method returns a new list to be used for finding median. When its done, returned list must be cleared
 */
public class RawLatencyResults {

    private final static Logger log = Logger.getLogger(LatencyStatistics.class);
    private static int MAX_SIZE = 10_000_000;

    static class LongHolder {
        long value;
        void clear() {
            value = 0;
        }
    }

    private LongHolder[] results = new LongHolder[MAX_SIZE];
    private int size;

    RawLatencyResults() {
        for (int i = 0; i < MAX_SIZE; i++) {
            results[i] = new LongHolder();
        }
    }

    void clear() {
        for (int i = 0; i < size; i++) {
            results[i].clear();
        }
        size = 0;
    }

    void add(long result) {
        if (size == MAX_SIZE) {
            MAX_SIZE *= 2;
            results = Arrays.copyOf(results, MAX_SIZE);
            log.warn("Raw latency results array size increased to " + MAX_SIZE);
        }
        results[size++].value = result;
    }

    void dump(BufferedWriter writer) throws IOException {
        for (int i = 0; i < size; i++) {
            writer.write(Long.toString(results[i].value / 1000));
            writer.newLine();
        }
    }

    List<LongHolder> getSortedResults() {
        List<LongHolder> sortedResults = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            sortedResults.add(results[i]);
        }
        sortedResults.sort(Comparator.comparingLong(lh -> lh.value));
        return sortedResults;
    }
}
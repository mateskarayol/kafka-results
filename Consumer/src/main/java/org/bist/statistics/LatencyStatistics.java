package org.bist.statistics;

import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static java.lang.Math.max;
import static java.lang.Math.min;


public class LatencyStatistics implements Statistics {

    private final static int NUMBER_OF_DISTRIBUTION_BUCKETS = 21;
    private final static Logger log = Logger.getLogger(LatencyStatistics.class);

    private long minTime = 0;
    private long maxTime = 0;
    private int numberOfSamples = 0;
    private int bucketSize; // nanoseconds

    private long aggregatedTime = 0;
    private int[] distribution = new int[NUMBER_OF_DISTRIBUTION_BUCKETS];
    private RawLatencyResults rawLatencyResults = new RawLatencyResults();

    public LatencyStatistics(int bucketSize) { // take in microseconds
        this.bucketSize = bucketSize * 1_000;
        clear();
    }

    @Override
    public void transactionCompleted(long startTime, long endTime) {

        transactionCompleted(endTime - startTime);
    }

    @Override
    public void transactionCompleted(long executionTime) {

        aggregatedTime += executionTime;
        minTime = min(minTime, executionTime);
        maxTime = max(maxTime, executionTime);
        numberOfSamples++;

        addToDistribution(executionTime);
        rawLatencyResults.add(executionTime);
    }

    @Override
    public String getSummary() {

        StringBuilder sb = new StringBuilder();

        List<RawLatencyResults.LongHolder> results = rawLatencyResults.getSortedResults();

        sb.append(String.format("samples=%4d, bucketSize=%4d, min=%4d, avg=%4d, median=%5d, 95=%5d, 99=%5d, max=%5d",
                numberOfSamples, bucketSize / 1_000, getMin(), getAvg(), getMedian(results),
                getConfidenceResult(results, 95), getConfidenceResult(results, 99), getMax()));

        appendBuckets(sb);

        results.clear();

        return sb.toString();
    }

    @Override
    public String getSummaryForExcel() {

        StringBuilder sb = new StringBuilder();

        List<RawLatencyResults.LongHolder> results = rawLatencyResults.getSortedResults();

        sb.append(String.format("%d,%d,%d,%d,%d,%d,%d", getMin(), getAvg(), getMedian(results),
                getConfidenceResult(results, 95), getConfidenceResult(results, 99), getMax(), numberOfSamples));

        appendBuckets(sb);

        results.clear();

        return sb.toString();
    }

    @Override
    public void dumpRawData(String fileName) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            rawLatencyResults.dump(writer);
        }
    }

    private void appendBuckets(StringBuilder sb) {

        for (int i = 0; i < distribution.length; i++) {
            if (i == 0) { // First one
                sb.append(",[");
            }
            sb.append(distribution[i]);
            if (i == distribution.length - 1) { // Last one
                sb.append("]");
            } else {
                sb.append("-");
            }
        }
    }

    @Override
    public void clear() {
        minTime = Long.MAX_VALUE;
        maxTime = 0;
        aggregatedTime = 0;
        numberOfSamples = 0;
        Arrays.fill(distribution, 0);
        rawLatencyResults.clear();
    }

    private long getMin() {
        return minTime / 1000;
    }

    private long getMax() {
        return maxTime / 1000;
    }

    private long getAvg() {
        return numberOfSamples > 0 ? aggregatedTime / 1000 / numberOfSamples : 0;
    }

    private long getMedian(List<RawLatencyResults.LongHolder> results) {
        if (results.size() % 2 == 0) {
            return results.size() >= 2 ? (results.get(results.size() / 2).value + results.get(results.size() / 2 - 1).value) / 2 / 1000: 0;
        } else {
            return results.get(results.size() / 2).value / 1000;
        }
    }

    private long getConfidenceResult(List<RawLatencyResults.LongHolder> results, int confidenceRange) {
        if (results.size() > 1) {
            int index = (int) ((long) results.size() * confidenceRange / 100) - 1;
            return results.get(index).value / 1000;
        } else {
            return 0;
        }
    }

    private void addToDistribution(long executionTime) {
        int bucket;
        for (bucket = 0; bucket < distribution.length - 1; bucket++) {
            if (executionTime < (bucket + 1) * bucketSize) {
                distribution[bucket]++;
                break;
            }
        }
        if (bucket == distribution.length - 1) {
            distribution[bucket]++;
        }
        if (log.isDebugEnabled()) {
            log.debug("Exec: " + executionTime + " Bucket: " + bucket);
        }
    }
}
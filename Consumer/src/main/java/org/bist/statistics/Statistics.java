package org.bist.statistics;

import java.io.IOException;

public interface Statistics {

    void transactionCompleted(long startTime, long endTime);
    void transactionCompleted(long executionTime);
    String getSummary();
    String getSummaryForExcel();
    void dumpRawData(String fileName) throws IOException;
    void clear();
}
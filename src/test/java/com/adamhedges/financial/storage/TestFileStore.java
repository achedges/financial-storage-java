package com.adamhedges.financial.storage;

import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

public abstract class TestFileStore<T extends Comparable<T>> {

    protected final String symbol;
    protected final String datapath;
    protected final String indexpath;

    protected final FileStore<T> filestore;
    protected final List<T> testRecords = new ArrayList<>();

    protected final Random random = new Random();

    private final DataAdapter<T> adapter;

    public TestFileStore(String symbol, DataAdapter<T> adapter) {
        this.symbol = symbol;
        this.adapter = adapter;
        this.filestore = new FileStore<>(this.adapter);
        this.datapath = this.filestore.adapter.getDataFilePath(this.symbol);
        this.indexpath = this.filestore.adapter.getIndexFilePath(this.symbol);
    }

    protected void setup() {
        testRecords.clear();
        filestore.index.remove(symbol);
    }

    protected void teardown() throws IOException {
        Files.deleteIfExists(Paths.get(datapath));
        Files.deleteIfExists(Paths.get(indexpath));
    }

    protected void writeTestRecords(long date, int start, boolean skipRecordGen) {
        int n = getTestRecordCount();
        if (!skipRecordGen) {
            for (int i = 0; i < n; i++) {
                testRecords.add(generateRandomizedRecord(symbol, date, i));
            }
        }

        filestore.write(symbol, date, testRecords.subList(start, start + n));
    }

    protected abstract int getTestRecordCount();
    protected abstract T generateRandomizedRecord(String symbol, long date, int i);

    protected void TestFileStore_basicIO() {
        int n = getTestRecordCount();
        long date = 20240101;
        writeTestRecords(date, 0, false);
        List<T> output = filestore.read(symbol, date);
        Assertions.assertEquals(n, output.size(), "Record count mismatch");
        for (int i = 0; i < n; i++) {
            Assertions.assertEquals(testRecords.get(i), output.get(i), "Records not equal");
        }
    }

    protected void TestFileStore_newDateAppend() {
        int n = getTestRecordCount();
        long date1 = 20220103L;
        long date2 = 20220104L;
        writeTestRecords(date1, 0, false);
        writeTestRecords(date2, n, false);
        List<T> output = filestore.read(symbol, date1, date2);
        Assertions.assertEquals(n * 2, output.size(), "Record count mismatch");
        for (int i = 0; i < n * 2; i++) {
            Assertions.assertEquals(output.get(i), testRecords.get(i), "Records not equal");
        }
    }

    protected void TestFileStore_newDateInsert() {
        int n = getTestRecordCount();
        long date1 = 20220103L;
        long date2 = 20220104L;
        long date3 = 20220105L;
        writeTestRecords(date1, 0, false);
        writeTestRecords(date3, n, false);
        writeTestRecords(date2, n * 2, false);
        Collections.sort(testRecords);
        List<T> output = filestore.read(symbol, date1, date3);
        Assertions.assertEquals(n * 3, output.size(), "Record count mismatch");
        for (int i = 0; i < n * 3; i++) {
            Assertions.assertEquals(output.get(i), testRecords.get(i), "Records not equal");
        }
    }

    protected void TestFileStore_existingDateOverride() {
        int n = getTestRecordCount();
        long date1 = 20220103L;
        long date2 = 20220104L;
        writeTestRecords(date1, 0, false);
        writeTestRecords(date2, n, false);
        writeTestRecords(date1, 0, true);
        List<T> output = filestore.read(symbol, date1, date2);
        Assertions.assertEquals(n * 2, output.size(), "Record count mismatch");
        for (int i = 0; i < n * 2; i++) {
            Assertions.assertEquals(output.get(i), testRecords.get(i), "Records not equal");
        }
    }

    protected void TestFileStore_existingDateAddRecords() {
        int n = getTestRecordCount();
        long date1 = 20220103L;
        long date2 = 20220104L;
        long date3 = 20220105L;

        List<T> buffer1 = new ArrayList<>();
        IntStream.range(0, n).forEach(i -> buffer1.add(generateRandomizedRecord(symbol, date1, i)));

        List<T> buffer2 = new ArrayList<>();
        IntStream.range(0, n).forEach(i -> buffer2.add(generateRandomizedRecord(symbol, date2, i)));

        List<T> buffer3 = new ArrayList<>();
        IntStream.range(0, n).forEach(i -> buffer3.add(generateRandomizedRecord(symbol, date3, i)));

        filestore.write(symbol, date1, buffer1);
        filestore.write(symbol, date2, buffer2);
        filestore.write(symbol, date3, buffer3);
        filestore.index.persist();

        IntStream.range(0, 5).forEach(i -> buffer2.add(generateRandomizedRecord(symbol, date2, i + n)));

        filestore.write(symbol, date2, buffer2);
        filestore.index.persist();

        testRecords.clear();
        testRecords.addAll(buffer1);
        testRecords.addAll(buffer2);
        testRecords.addAll(buffer3);

        List<T> output = filestore.read(symbol, date1, date3);
        Assertions.assertEquals(testRecords.size(), output.size(), "Record count mismatch");
        for (int i = 0; i < testRecords.size(); i++) {
            Assertions.assertEquals(testRecords.get(i), output.get(i), "Records not equal");
        }
    }

    protected void TestFileStore_existingDateRemoveRecords() {
        int n = getTestRecordCount();
        long date1 = 20220103L;
        long date2 = 20220104L;
        long date3 = 20220105L;

        List<T> buffer1 = new ArrayList<>();
        IntStream.range(0, n).forEach(i -> buffer1.add(generateRandomizedRecord(symbol, date1, i)));

        List<T> buffer2 = new ArrayList<>();
        IntStream.range(0, n).forEach(i -> buffer2.add(generateRandomizedRecord(symbol, date2, i)));

        List<T> buffer3 = new ArrayList<>();
        IntStream.range(0, n).forEach(i -> buffer3.add(generateRandomizedRecord(symbol, date3, i)));

        filestore.write(symbol, date1, buffer1);
        filestore.write(symbol, date2, buffer2);
        filestore.write(symbol, date3, buffer3);
        filestore.index.persist();

        List<T> newBuffer2 = buffer2.subList(0, n - 3);
        filestore.write(symbol, date2, newBuffer2);
        filestore.index.persist();

        testRecords.clear();
        testRecords.addAll(buffer1);
        testRecords.addAll(newBuffer2);
        testRecords.addAll(buffer3);

        List<T> output = filestore.read(symbol, date1, date3);
        Assertions.assertEquals(testRecords.size(), output.size(), "Record count mismatch");
        for (int i = 0; i < testRecords.size(); i++) {
            Assertions.assertEquals(testRecords.get(i), output.get(i), "Records not equal");
        }
    }

    protected void TestFileStore_delete() {
        int n = getTestRecordCount();
        long date1 = 20220103L;
        long date2 = 20220104L;
        long date3 = 20220105L;

        writeTestRecords(date1, 0, false);
        writeTestRecords(date2, n, false);
        writeTestRecords(date3, n * 2, false);

        filestore.write(symbol, date2, new ArrayList<>());
        filestore.index.persist();

        testRecords.removeIf(r -> adapter.getItemDate(r) == date2);
        List<T> output = filestore.read(symbol, date1, date3);
        Assertions.assertEquals(testRecords.size(), output.size(), "Record count mismatch");
        for (int i = 0; i < testRecords.size(); i++) {
            Assertions.assertEquals(testRecords.get(i), output.get(i), "Records not equal");
        }
    }

}

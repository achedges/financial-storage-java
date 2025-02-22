package com.adamhedges.financial.storage.buffers;

import com.adamhedges.financial.storage.FileStore;
import com.adamhedges.financial.core.bars.PriceBar;
import com.adamhedges.financial.storage.SimpleDataAdapter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TestStorageBuffer {

    private static final String symbol = "TSTBUF";
    private static final long date1 = 20220103;
    private static final long date2 = 20220104;
    private static final int numRecords = 2;
    private static final FileStore<PriceBar> filestore = new FileStore<>(new SimpleDataAdapter());

    private static final String datapath = filestore.adapter.getDataFilePath(symbol);
    private static final String indexpath = filestore.adapter.getIndexFilePath(symbol);

    private static void writeTestData(long date) {
        List<PriceBar> items = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            items.add(new PriceBar(symbol, date, i + 930));
        }
        filestore.write(symbol, date, items);
        filestore.index.persist();
    }

    @BeforeAll
    public static void setup() {
        writeTestData(date1);
        writeTestData(date2);
    }

    @AfterAll
    public static void tearDown() throws IOException {
        Files.deleteIfExists(Paths.get(datapath));
        Files.deleteIfExists(Paths.get(indexpath));
    }

    @Test
    public void TestStorageBuffer_nonRefillable() {
        FeedForwardBuffer buffer = new StorageBuffer(symbol, date1, filestore);

        Optional<PriceBar> bar = buffer.getNext();
        Assertions.assertFalse(bar.isEmpty());
        Assertions.assertEquals(930, bar.get().getTime());
        Assertions.assertTrue(buffer.getPrev().isEmpty());

        bar = buffer.getNext();
        Assertions.assertFalse(bar.isEmpty());
        Assertions.assertEquals(931, bar.get().getTime());
        Assertions.assertFalse(buffer.getPrev().isEmpty());
        Assertions.assertEquals(930, buffer.getPrev().get().getTime());

        bar = buffer.getNext();
        Assertions.assertTrue(bar.isEmpty());
        Assertions.assertFalse(buffer.getPrev().isEmpty());
        Assertions.assertEquals(931, buffer.getPrev().get().getTime());

        Optional<PriceBar> peekBar = buffer.peek();
        Assertions.assertTrue(peekBar.isPresent());
        Assertions.assertEquals(931, peekBar.get().getTime());
    }

}

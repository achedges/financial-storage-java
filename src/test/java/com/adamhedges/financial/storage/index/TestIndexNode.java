package com.adamhedges.financial.storage.index;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIndexNode {

    @Test
    public void TestIndexNode_fromCsv() {
        long date = 20240101L;
        long offset = 1024;
        int count = 390;

        String csv = String.format("%s,%s,%s%n", date, offset, count);
        IndexNode node = IndexNode.fromCsv(csv);

        Assertions.assertEquals(date, node.getDate());
        Assertions.assertEquals(offset, node.getOffset());
        Assertions.assertEquals(count, node.getCount());
    }

    @Test
    public void TestIndexNode_toCsv() {
        IndexNode node = new IndexNode(20240101L, 1024, 390);
        Assertions.assertEquals("20240101,1024,390", node.toCsv());
    }

}

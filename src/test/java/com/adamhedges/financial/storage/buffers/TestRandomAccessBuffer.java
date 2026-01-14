package com.adamhedges.financial.storage.buffers;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class TestRandomAccessBuffer {

    @Test
    public void TestRandomAccessBuffer_init() {
        RandomAccessBuffer<Integer> buffer = new RandomAccessBuffer<>(List.of(0, 1, 2, 3, 4));
        Assertions.assertEquals(5, buffer.getBufferSize());
        Assertions.assertEquals(4, buffer.getCurrentBufferIndex());
    }

    @Test
    public void TestRandomAccessBuffer_getPosition() {
        RandomAccessBuffer<Integer> buffer = new RandomAccessBuffer<>(List.of(0, 1, 2));
        Assertions.assertEquals(2, buffer.getPosition(0));
        Assertions.assertEquals(1, buffer.getPosition(1));
        Assertions.assertEquals(0, buffer.getPosition(2));
        Assertions.assertEquals(-1, buffer.getPosition(3));
        Assertions.assertEquals(-1, buffer.getPosition(4));
    }

    @Test
    public void TestRandomAccessBuffer_getLast() {
        RandomAccessBuffer<Integer> buffer = new RandomAccessBuffer<>(List.of(0, 1, 2));
        Assertions.assertEquals(2, buffer.getLast().orElse(-1));
        Assertions.assertEquals(2, buffer.getLast(0).orElse(-1));
        Assertions.assertEquals(1, buffer.getLast(1).orElse(-1));
        Assertions.assertEquals(0, buffer.getLast(2).orElse(-1));
        Assertions.assertTrue(buffer.getLast(4).isEmpty());
    }

    @Test
    public void TestRandomAccessBuffer_add() {
        RandomAccessBuffer<Integer> buffer = new RandomAccessBuffer<>(List.of(0, 1, 2));
        Assertions.assertEquals(2, buffer.getLast().orElse(-1));

        buffer.add(3);
        Assertions.assertEquals(3, buffer.getLast().orElse(-1));
        Assertions.assertEquals(2, buffer.getLast(1).orElse(-1));
        Assertions.assertEquals(1, buffer.getLast(2).orElse(-1));
        Assertions.assertTrue(buffer.getLast(3).isEmpty());

        buffer.add(4);
        Assertions.assertEquals(4, buffer.getLast().orElse(-1));
        Assertions.assertEquals(3, buffer.getLast(1).orElse(-1));
        Assertions.assertEquals(2, buffer.getLast(2).orElse(-1));
        Assertions.assertTrue(buffer.getLast(3).isEmpty());

        // exercise the wrap-around a bit more
        RandomAccessBuffer<Integer> buffer2 = new RandomAccessBuffer<>(List.of(0, 1, 2));
        for (int i = 3; i <= 10; i++) {
            buffer2.add(i);
            Assertions.assertEquals(i, buffer2.getLast().orElse(-1));
        }
    }

}

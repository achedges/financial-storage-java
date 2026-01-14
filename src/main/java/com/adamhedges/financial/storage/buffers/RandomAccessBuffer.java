package com.adamhedges.financial.storage.buffers;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RandomAccessBuffer<T> {

    private final List<T> elements;
    private final int size;
    private int index;

    RandomAccessBuffer(List<T> elements) {
        this.elements = new ArrayList<>(elements);
        this.size = elements.size();
        this.index = this.size - 1;
    }

    public int getBufferSize() {
        return size;
    }

    public int getCurrentBufferIndex() {
        return index;
    }

    public int getPosition(int offset) {
        if (offset >= size) {
            return -1;
        } else {
            return (index - offset) % size;
        }
    }

    public Optional<T> getLast() {
        return getLast(0);
    }

    public Optional<T> getLast(int offset) {
        int pos = getPosition(offset);
        return pos >= 0 ? Optional.of(elements.get(pos)) : Optional.empty();
    }

    public void add(T element) {
        index++;
        elements.set(getPosition(0), element);
    }

}

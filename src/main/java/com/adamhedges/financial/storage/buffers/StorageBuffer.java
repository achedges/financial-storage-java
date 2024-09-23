package com.adamhedges.financial.storage.buffers;

import com.adamhedges.financial.storage.FileStore;
import com.adamhedges.financial.core.bars.PriceBar;
import com.adamhedges.utilities.datetime.DateUtilities;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public class StorageBuffer extends FeedForwardBuffer {

    private int index = 0;
    private final List<PriceBar> bars;

    public StorageBuffer(String symbol, long date, FileStore<PriceBar> filestore) {
        super();
        this.bars = filestore.read(symbol, date);
    }

    @Override
    public Optional<PriceBar> getNext() {
        return getNext(DateUtilities.getZonedNowInstant(DateUtilities.EASTERN_TIMEZONE));
    }

    @Override
    public Optional<PriceBar> getNext(Instant timestamp) {
        prev = next;

        if (index == bars.size() || bars.isEmpty()) {
            return Optional.empty();
        }

        next = bars.get(index);
        index++;

        return Optional.of(next);
    }

    @Override
    public Optional<PriceBar> getPrev() {
        return prev == null ? Optional.empty() : Optional.of(prev);
    }

    @Override
    public int getSize() {
        return bars.size();
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public void clear() {
        index = 0;
        bars.clear();
    }

}

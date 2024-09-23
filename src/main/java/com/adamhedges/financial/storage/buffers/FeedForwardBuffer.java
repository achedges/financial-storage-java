package com.adamhedges.financial.storage.buffers;

import com.adamhedges.financial.core.bars.PriceBar;

import java.time.Instant;
import java.util.Optional;

public abstract class FeedForwardBuffer {

    protected PriceBar next = null;
    protected PriceBar prev = null;

    public abstract Optional<PriceBar> getNext();
    public abstract Optional<PriceBar> getNext(Instant timestamp);
    public abstract Optional<PriceBar> getPrev();
    public abstract int getSize();
    public abstract int getIndex();
    public abstract void clear();

}

package com.adamhedges.financial.storage.buffers;

import com.adamhedges.financial.core.bars.PriceBar;
import com.adamhedges.utilities.datetime.DateUtilities;
import com.adamhedges.utilities.logger.Logger;
import lombok.Getter;

import java.time.Instant;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Semaphore;

public class AggregateBuffer extends FeedForwardBuffer {

    @Getter
    private final String symbol;

    private final Logger logger;

    private final Semaphore lock = new Semaphore(1);
    private final Queue<PriceBar> buffer = new LinkedList<>();

    @Getter
    private PriceBar lastBar;

    public AggregateBuffer(String symbol) {
        super();
        this.symbol = symbol;
        this.logger = null;
    }

    public AggregateBuffer(String symbol, Logger logger) {
        super();
        this.symbol = symbol;
        this.logger = logger;
    }

    public void set(PriceBar bar) {
        try {
            tryLog(String.format("Buffering bar %s [%s ]", symbol, bar));
            lock.acquire();
            buffer.add(bar);
        } catch (Exception ignored) {

        } finally {
            lock.release();
        }
    }

    @Override
    public Optional<PriceBar> getNext() {
        return getNext(DateUtilities.getZonedNowInstant(DateUtilities.EASTERN_TIMEZONE));
    }

    @Override
    public Optional<PriceBar> getNext(Instant timestamp) {
        Optional<PriceBar> ret = Optional.empty();

        try {
            lock.acquire();
            PriceBar nextBar = buffer.poll();
            if (nextBar != null) {
                tryLog(String.format("Captured bar %s [ %s ]", symbol, nextBar));
                prev = lastBar;
                lastBar = nextBar;
                ret = Optional.of(nextBar);
            }
        } catch (Exception ignored) {

        } finally {
            lock.release();
        }

        return ret;
    }

    @Override
    public Optional<PriceBar> getPrev() {
        return Optional.of(prev);
    }

    @Override
    public int getSize() {
        return buffer.size();
    }

    @Override
    public int getIndex() {
        return 0;
    }

    @Override
    public void clear() {
        buffer.clear();
        next = null;
        prev = null;
    }

    private void tryLog(String message) {
        if (logger == null) {
            return;
        }

        logger.log(message);
    }

}

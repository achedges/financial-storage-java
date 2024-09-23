package com.adamhedges.financial.storage.buffers;

import com.adamhedges.financial.core.bars.PriceBar;
import com.adamhedges.utilities.datetime.DateUtilities;
import com.adamhedges.utilities.logger.Logger;
import lombok.Getter;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Semaphore;

public class AggregateBuffer extends FeedForwardBuffer {

    @Getter
    private final String symbol;

    private final Logger logger;

    private final Semaphore lock = new Semaphore(1);
    private PriceBar currentBar = null;

    @Getter
    private double lastPrice;
    @Getter
    private PriceBar lastBar;

    private int lastCheckMinute = 0;

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

    public void set(double price, Instant timestamp) {
        lastPrice = price;

        try {
            lock.acquire();
            if (currentBar == null) {
                currentBar = new PriceBar(symbol, price, timestamp);
                tryLog(String.format("Initialized bar %s [ %s ]", symbol, currentBar));
            }

            if (price > currentBar.getHigh()) {
                currentBar.setHigh(price);
            } else if (price < currentBar.getLow()) {
                currentBar.setLow(price);
            }

            currentBar.setClose(price);
        } catch (Exception ignored) {

        } finally {
            lock.release();
        }
    }

    public void set(PriceBar bar) {
        lastBar = bar;

        try {
            lock.acquire();
            if (currentBar == null) {
                currentBar = bar;
            } else {
                long totalVolume = currentBar.getVolume() + bar.getVolume();
                double currentBarTotal = currentBar.getVwap() * currentBar.getVolume();
                double barTotal = bar.getVwap() * bar.getVolume();
                currentBar.setVolume(totalVolume);
                currentBar.setVwap((currentBarTotal + barTotal) / totalVolume);
            }

            if (bar.getHigh() > currentBar.getHigh()) {
                currentBar.setHigh(bar.getHigh());
            } else if (bar.getLow() < currentBar.getLow()) {
                currentBar.setLow(bar.getLow());
            }

            currentBar.setClose(bar.getClose());

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
        int time = DateUtilities.getInstantTime(timestamp, DateUtilities.EASTERN_TIMEZONE);

        try {
            lock.acquire();

            if (currentBar != null && time != currentBar.getTime()) {
                tryLog(String.format("Captured bar %s [ %s ]", symbol, currentBar));
                prev = currentBar;
                ret = Optional.of(currentBar);
                currentBar = null;
            } else if (time != lastCheckMinute && prev != null) {
                tryLog(String.format("Replaying bar %s [ %s ]", symbol, prev));
                ret = Optional.of(prev);
            }
        } catch (Exception ignored) {

        } finally {
            lastCheckMinute = time;
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
        return 1;
    }

    @Override
    public int getIndex() {
        return 0;
    }

    @Override
    public void clear() {
        currentBar = null;
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

package com.adamhedges.financial.storage.buffers;

import com.adamhedges.financial.core.bars.PriceBar;
import com.adamhedges.utilities.datetime.DateUtilities;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.util.Optional;

public class TestAggregateBuffer {

    private PriceBar getBar(ZonedDateTime timestamp, double basisPrice, long volume) {
        PriceBar bar = new PriceBar("TEST", basisPrice, timestamp.toInstant());
        bar.setVolume(volume);
        return bar;
    }

    @Test
    public void TestAggregateBuffer_set() {
        AggregateBuffer buffer = new AggregateBuffer("TEST");

        ZonedDateTime date1 = ZonedDateTime.of(2024, 1, 22, 9, 30, 0, 0, DateUtilities.EASTERN_TIMEZONE);
        ZonedDateTime date2 = ZonedDateTime.of(2024, 1, 22, 9, 31, 0, 0, DateUtilities.EASTERN_TIMEZONE);

        buffer.set(10.0, date1.toInstant());
        Assertions.assertTrue(buffer.getNext(date1.toInstant()).isEmpty());
        buffer.set(11.0, date1.toInstant());
        Assertions.assertTrue(buffer.getNext(date1.toInstant()).isEmpty());
        buffer.set(9.0, date1.toInstant());
        Assertions.assertTrue(buffer.getNext(date1.toInstant()).isEmpty());

        Optional<PriceBar> firstBar = buffer.getNext(date2.toInstant());

        Assertions.assertFalse(firstBar.isEmpty());
        Assertions.assertEquals(10.0, firstBar.get().getOpen());
        Assertions.assertEquals(11.0, firstBar.get().getHigh());

        // low and close will actually be the first date2 price
        Assertions.assertEquals(9.0, firstBar.get().getLow());
        Assertions.assertEquals(9.0, firstBar.get().getClose());

        Assertions.assertTrue(buffer.getNext(date2.toInstant()).isEmpty());
    }

    @Test
    public void TestAggregateBuffer_setBar() {
        AggregateBuffer buffer = new AggregateBuffer("TEST");

        ZonedDateTime date1 = ZonedDateTime.of(2024, 1, 22, 9, 30, 0, 0, DateUtilities.EASTERN_TIMEZONE);
        ZonedDateTime date2 = ZonedDateTime.of(2024, 1, 22, 9, 31, 0, 0, DateUtilities.EASTERN_TIMEZONE);

        buffer.set(getBar(date1, 10.0, 20));
        Assertions.assertTrue(buffer.getNext(date1.toInstant()).isEmpty());

        buffer.set(getBar(date1, 11.0, 10));
        Assertions.assertTrue(buffer.getNext(date1.toInstant()).isEmpty());

        buffer.set(getBar(date1, 9.0, 5));
        Assertions.assertTrue(buffer.getNext(date1.toInstant()).isEmpty());

        Optional<PriceBar> firstBar = buffer.getNext(date2.toInstant());
        Assertions.assertFalse(firstBar.isEmpty());
        Assertions.assertEquals(10.0, firstBar.get().getOpen());
        Assertions.assertEquals(11.0, firstBar.get().getHigh());
        Assertions.assertEquals(9.0, firstBar.get().getLow());
        Assertions.assertEquals(9.0, firstBar.get().getClose());
        Assertions.assertEquals(35, firstBar.get().getVolume());
        Assertions.assertEquals(10.14, firstBar.get().getVwap(), 0.01);
    }

}

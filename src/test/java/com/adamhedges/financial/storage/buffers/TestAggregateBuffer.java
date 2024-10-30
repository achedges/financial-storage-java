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
    public void TestAggregateBuffer_setBar() {
        AggregateBuffer buffer = new AggregateBuffer("TEST");

        ZonedDateTime date = ZonedDateTime.of(2024, 1, 22, 9, 30, 0, 0, DateUtilities.EASTERN_TIMEZONE);
        PriceBar bar1 = getBar(date, 10.0, 100);
        PriceBar bar2 = getBar(date, 11.0, 50);

        buffer.set(bar1);
        Assertions.assertEquals(1, buffer.getSize());

        buffer.set(bar2);
        Assertions.assertEquals(2, buffer.getSize());

        Optional<PriceBar> bar = buffer.getNext();
        Assertions.assertTrue(bar.isPresent());
        Assertions.assertEquals(bar1.getClose(), bar.get().getClose());
        Assertions.assertEquals(1, buffer.getSize());

        bar = buffer.getNext();
        Assertions.assertTrue(bar.isPresent());
        Assertions.assertEquals(bar2.getClose(), bar.get().getClose());
        Assertions.assertEquals(0, buffer.getSize());

        Assertions.assertEquals(bar1.getClose(), buffer.getPrev().orElseThrow(() -> new RuntimeException("No previous bar")).getClose());
        Assertions.assertEquals(bar2.getClose(), buffer.getLastBar().getClose());
    }

}

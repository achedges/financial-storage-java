package com.adamhedges.financial.storage.helpers;

import com.adamhedges.financial.storage.FileStore;
import com.adamhedges.financial.storage.index.IndexNode;
import com.adamhedges.financial.core.bars.PriceBar;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PriceBarFileStoreHelpers {

    public static List<PriceBar> getBackfillBars(FileStore<PriceBar> fileStore, String symbol, int numBars) {
        return getBackfillBars(fileStore, symbol, numBars, null);
    }

    public static List<PriceBar> getBackfillBars(FileStore<PriceBar> fileStore, String symbol, int numBars, Long beforeDate) {

        Optional<IndexNode> dateNode = fileStore.index.last(symbol);
        if (dateNode.isEmpty()) {
            return new ArrayList<>();
        }

        long date = dateNode.get().getDate();

        if (beforeDate != null && beforeDate <= dateNode.get().getDate()) {
            Optional<IndexNode> beforeDateNode = fileStore.index.prev(symbol, beforeDate);
            if (beforeDateNode.isPresent()) {
                date = beforeDateNode.get().getDate();
            }
        }

        List<PriceBar> bars = fileStore.read(symbol, date);
        int n = Math.min(numBars, bars.size());
        return bars.subList(bars.size() - n, bars.size());

    }

}

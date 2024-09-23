package com.adamhedges.financial.storage.index;

import com.adamhedges.financial.storage.DataAdapter;
import com.adamhedges.financial.core.bars.PriceBar;
import com.adamhedges.financial.storage.SimpleDataAdapter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class TestSymbolIndex {

    private final String symbol = "TSTIDX";

    @Test
    public void TestSymbolIndex_lookup() {
        DataAdapter<PriceBar> adapter  = new SimpleDataAdapter();
        SymbolIndex<PriceBar> symbolIndex = new SymbolIndex<>(adapter);

        Assertions.assertEquals(Optional.empty(), symbolIndex.lookup("BOGUS", 20240101L));
        Assertions.assertEquals(Optional.empty(), symbolIndex.lookup(symbol, 20231231L));

        Optional<IndexNode> lookupNode = symbolIndex.lookup(symbol, 20240102L);
        Assertions.assertTrue(lookupNode.isPresent());
        Assertions.assertEquals(20240102L, lookupNode.get().getDate());
        Assertions.assertEquals(560, lookupNode.get().getOffset());
        Assertions.assertEquals(10, lookupNode.get().getCount());
    }

    @Test
    public void TestSymbolIndex_first() {
        DataAdapter<PriceBar> adapter = new SimpleDataAdapter();
        SymbolIndex<PriceBar> symbolIndex = new SymbolIndex<>(adapter);
        Optional<IndexNode> first = symbolIndex.first(symbol);
        Assertions.assertTrue(first.isPresent());
        Assertions.assertEquals(20240101L, first.get().getDate());
    }

    @Test
    public void TestSymbolIndex_last() {
        DataAdapter<PriceBar> adapter = new SimpleDataAdapter();
        SymbolIndex<PriceBar> symbolIndex = new SymbolIndex<>(adapter);
        Optional<IndexNode> last = symbolIndex.last(symbol);
        Assertions.assertTrue(last.isPresent());
        Assertions.assertEquals(20240104L, last.get().getDate());
    }

    @Test
    public void TestSymbolIndex_next() {
        DataAdapter<PriceBar> adapter = new SimpleDataAdapter();
        SymbolIndex<PriceBar> symbolIndex = new SymbolIndex<>(adapter);
        Optional<IndexNode> next = symbolIndex.next(symbol, 20240102L);
        Assertions.assertTrue(next.isPresent());
        Assertions.assertEquals(20240103L, next.get().getDate());

        next = symbolIndex.next(symbol, 20240104L);
        Assertions.assertFalse(next.isPresent());
    }

    @Test
    public void TestSymbolIndex_prev() {
        DataAdapter<PriceBar> adapter = new SimpleDataAdapter();
        SymbolIndex<PriceBar> symbolIndex = new SymbolIndex<>(adapter);
        Optional<IndexNode> prev = symbolIndex.prev(symbol, 20240103L);
        Assertions.assertTrue(prev.isPresent());
        Assertions.assertEquals(20240102L, prev.get().getDate());

        prev = symbolIndex.prev(symbol, 20240101L);
        Assertions.assertFalse(prev.isPresent());
    }

    @Test
    public void TestSymbolIndex_persist() {
        DataAdapter<PriceBar> adapter = new SimpleDataAdapter();
        SymbolIndex<PriceBar> index = new SymbolIndex<>(adapter);
        index.load(symbol);

        // add new record to index
        index.get(symbol).put(20231231L, new IndexNode(20231231L, 0, 10));
        index.get(symbol).setDirty();
        index.persist();

        // reset to reload
        index.remove(symbol);

        // lookup
        Optional<IndexNode> newnode = index.lookup(symbol, 20231231L);
        Assertions.assertTrue(newnode.isPresent());
        Assertions.assertEquals(20231231L, newnode.get().getDate());
        Assertions.assertEquals(0, newnode.get().getOffset());
        Assertions.assertEquals(10, newnode.get().getCount());

        // reset for subsequent tests
        index.get(symbol).remove(20231231L);
        index.get(symbol).setDirty();
        index.persist();
    }

}

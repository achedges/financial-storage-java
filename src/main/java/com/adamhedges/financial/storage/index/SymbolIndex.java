package com.adamhedges.financial.storage.index;

import com.adamhedges.financial.storage.DataAdapter;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class SymbolIndex<T extends Comparable<T>> extends HashMap<String, DateIndex> {

    private final DataAdapter<T> adapter;

    public SymbolIndex(DataAdapter<T> dataAdapter) {
        adapter = dataAdapter;
    }

    public void load(String symbol) {
        if (this.containsKey(symbol)) {
            return;
        }

        this.put(symbol, new DateIndex());

        try {
            String filepath = adapter.getIndexFilePath(symbol);
            List<String> indexLines = Files.readAllLines(Paths.get(filepath));
            for (String line : indexLines) {
                IndexNode node = IndexNode.fromCsv(line);
                this.get(symbol).put(node.getDate(), node);
            }
        } catch (IOException ignored) { }
    }

    public void persist() {
        for (Entry<String, DateIndex> entry : this.entrySet()) {
            String symbol = entry.getKey();
            DateIndex dateIndex = entry.getValue();
            if (dateIndex.isDirty()) {
                String filepath = adapter.getIndexFilePath(symbol);
                try (FileWriter writer = new FileWriter(filepath, false)) {
                    for (IndexNode node : dateIndex.values()) {
                        if (node.getCount() > 0) {
                            writer.write(String.format("%s%n", node.toCsv()));
                        }
                    }
                    dateIndex.setClean();
                } catch (IOException ioex) {
                    System.out.printf("Unable to write %s index file: %s%n", symbol, ioex.getMessage());
                }
            }
        }
    }

    public Optional<IndexNode> lookup(String symbol, Long date) {
        load(symbol);
        if (!this.containsKey(symbol)) {
            return Optional.empty();
        }

        if (!this.get(symbol).containsKey(date)) {
            return Optional.empty();
        }

        return Optional.of(this.get(symbol).get(date));
    }

    public Optional<IndexNode> first(String symbol) {
        load(symbol);
        if (!this.containsKey(symbol) || this.get(symbol).isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(this.get(symbol).firstEntry().getValue());
    }

    public Optional<IndexNode> last(String symbol) {
        load(symbol);
        if (!this.containsKey(symbol) || this.get(symbol).isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(this.get(symbol).lastEntry().getValue());
    }

    public Optional<IndexNode> prev(String symbol, long date) {
        load(symbol);
        if (!this.containsKey(symbol) || this.get(symbol).isEmpty()) {
            return Optional.empty();
        }

        Long nullableDateKey = this.get(symbol).lowerKey(date);
        return nullableDateKey == null ? Optional.empty() : Optional.of(this.get(symbol).get(nullableDateKey));
    }

    public Optional<IndexNode> next(String symbol, long date) {
        load(symbol);
        if (!this.containsKey(symbol) || this.get(symbol).isEmpty()) {
            return Optional.empty();
        }

        Long nullableDateKey = this.get(symbol).higherKey(date);
        return nullableDateKey == null ? Optional.empty() : Optional.of(this.get(symbol).get(nullableDateKey));
    }

}

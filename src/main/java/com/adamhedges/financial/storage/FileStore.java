package com.adamhedges.financial.storage;

import com.adamhedges.financial.storage.exceptions.FileStoreIntegrityException;
import com.adamhedges.financial.storage.index.IndexNode;
import com.adamhedges.financial.storage.index.SymbolIndex;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class FileStore<T extends Comparable<T>> {

    public final DataAdapter<T> adapter;
    public final SymbolIndex<T> index;

    public FileStore(DataAdapter<T> dataAdapter) {
        adapter = dataAdapter;
        index = new SymbolIndex<>(adapter);
    }

    private void shiftBytes(RandomAccessFile file, long offset, int count, int deltaBytes) throws IOException {
        byte[] bytes = new byte[count * adapter.getRecordSizeBytes()];
        file.seek(offset);
        file.read(bytes, 0, bytes.length);
        file.seek(offset + deltaBytes);
        file.write(bytes, 0, bytes.length);
    }

    private void writeBytes(RandomAccessFile file, List<T> items, long offset) throws IOException {
        file.seek(offset);
        for (T item : items) {
            adapter.toByteBuffer(item);
            file.write(adapter.buffer.array());
        }
    }

    private void writeNewItems(String symbol, long date, List<T> items, RandomAccessFile file) throws IOException {
        IndexNode newnode = new IndexNode(date, 0, items.size());
        Optional<IndexNode> lastnodeopt = index.last(symbol);

        if (lastnodeopt.isPresent()) {
            IndexNode lastnode = lastnodeopt.get();
            if (newnode.isAfter(lastnode)) {
                // append to the end of the file
                newnode.setOffset(lastnode.getOffset() + adapter.getRecordSizeBytes(lastnode.getCount()));
            } else {
                // shift newer records and insert into the file
                int deltabytes = adapter.getRecordSizeBytes(newnode.getCount());
                Optional<IndexNode> shiftnode = Optional.of(lastnode);
                while (shiftnode.isPresent() && newnode.isBefore(shiftnode.get())) {
                    shiftBytes(file, shiftnode.get().getOffset(), shiftnode.get().getCount(), deltabytes);
                    shiftnode.get().updateOffset(deltabytes);
                    shiftnode = index.prev(symbol, shiftnode.get().getDate());
                }

                shiftnode.ifPresent(n -> newnode.setOffset(n.getOffset() + adapter.getRecordSizeBytes(n.getCount())));
            }
        }

        index.get(symbol).put(newnode.getDate(), newnode);
        writeBytes(file, items, newnode.getOffset());
    }

    private void writeExistingItems(String symbol, IndexNode updateNode, List<T> items, RandomAccessFile file) throws IOException {
        if (updateNode.getCount() < items.size()) {
            // new buffer has more data, need to expand the file
            Optional<IndexNode> shiftnode = index.last(symbol);
            int deltabytes = adapter.getRecordSizeBytes(items.size() - updateNode.getCount());

            while (shiftnode.isPresent() && shiftnode.get().isAfter(updateNode)) {
                shiftBytes(file, shiftnode.get().getOffset(), shiftnode.get().getCount(), deltabytes);
                shiftnode.get().updateOffset(deltabytes);
                shiftnode = index.prev(symbol, shiftnode.get().getDate());
            }

            updateNode.setCount(items.size());
        } else if (updateNode.getCount() > items.size()) {
            // new buffer has less data, need to compact the file
            Optional<IndexNode> shiftnode = index.next(symbol, updateNode.getDate());
            int deltabytes = adapter.getRecordSizeBytes(items.size() - updateNode.getCount());

            while (shiftnode.isPresent()) {
                shiftBytes(file, shiftnode.get().getOffset(), shiftnode.get().getCount(), deltabytes);
                shiftnode.get().updateOffset(deltabytes);
                shiftnode = index.next(symbol, shiftnode.get().getDate());
            }

            updateNode.setCount(items.size());
            file.setLength(file.getFilePointer());
        }

        writeBytes(file, items, updateNode.getOffset());
    }

    public void write(String symbol, long date, List<T> items) {
        index.load(symbol);
        Optional<IndexNode> writenode = index.lookup(symbol, date);

        // sort the items
        Collections.sort(items);

        // open or create symbol data file
        String filename = adapter.getDataFilePath(symbol);
        try (RandomAccessFile file = new RandomAccessFile(filename, "rw")) {

            if (writenode.isEmpty()) {
                writeNewItems(symbol, date, items, file);
            } else {
                writeExistingItems(symbol, writenode.get(), items, file);
            }

            index.get(symbol).setDirty();

        } catch (IOException ioex) {
            System.out.printf("Unable to write %s data file: %s%n", symbol, ioex.getMessage());
        }

        index.persist();
    }

    public List<T> read(String symbol, long date) {
        return read(symbol, date, date);
    }

    public List<T> read(String symbol, long fromDate, long throughDate) {
        index.load(symbol);
        List<T> items = new ArrayList<>();

        Optional<IndexNode> datenode = index.lookup(symbol, fromDate);
        if (datenode.isEmpty()) {
            return items;
        }

        String filename = adapter.getDataFilePath(symbol);
        try (RandomAccessFile file = new RandomAccessFile(filename, "r")) {
            while (datenode.get().getDate() <= throughDate) {
                file.seek(datenode.get().getOffset());
                for (int i = 0; i < datenode.get().getCount(); i++) {
                    T item = adapter.read(file, symbol);
                    if (item != null) {
                        items.add(item);
                    }
                }

                datenode = index.next(symbol, datenode.get().getDate());
                if (datenode.isEmpty()) {
                    break;
                }
            }
        } catch (IOException ex) {
            System.out.printf("Unable to open %s data file: %s%n", symbol, ex.getMessage());
        }

        return items;
    }

    public void checkIntegrity(String symbol) throws FileStoreIntegrityException {

        String filename = adapter.getDataFilePath(symbol);
        try (RandomAccessFile file = new RandomAccessFile(filename, "r")) {

            // read first record and rewind
            T item = adapter.read(file, symbol);
            file.seek(0);

            long lastid = 0;
            int numprices = 0;
            long date = adapter.getItemDate(item);

            Optional<IndexNode> indexNode = index.lookup(symbol, date);

            while ((item = adapter.read(file, symbol)) != null) {

                if (indexNode.isEmpty()) {
                    throw new FileStoreIntegrityException(symbol, String.format("Unable to resolve index node for date %s", date));
                }

                if (adapter.getItemDate(item) != date) {
                    if (numprices != indexNode.get().getCount()) {
                        throw new FileStoreIntegrityException(symbol, String.format("Index count mismatch for date %s", date));
                    }
                    date = adapter.getItemDate(item);
                    indexNode = index.lookup(symbol, date);
                    numprices = 1;
                } else {
                    numprices++;
                }

                if (adapter.getItemId(item) <= lastid) {
                    throw new FileStoreIntegrityException(symbol, String.format("Non-increasing ID detected for date %s", date));
                }

                lastid = adapter.getItemId(item);

            }

        } catch (IOException ioex) {
            throw new FileStoreIntegrityException(symbol, ioex.getMessage());
        }

    }

}

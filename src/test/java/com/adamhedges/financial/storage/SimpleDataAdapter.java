package com.adamhedges.financial.storage;

import com.adamhedges.financial.core.bars.PriceBar;

import java.io.IOException;
import java.io.RandomAccessFile;

public class SimpleDataAdapter extends DataAdapter<PriceBar> {

    public SimpleDataAdapter() {
        super("", "data/minute", "index/minute");
    }

    @Override
    public int getRecordSizeBytes() {
        return getRecordSizeBytes(1);
    }

    @Override
    public int getRecordSizeBytes(int numRecords) {
        return 16 * numRecords;
    }

    @Override
    public void toByteBuffer(PriceBar item) {
        buffer.rewind();
        buffer.putLong(item.getId());
        buffer.putDouble(item.getOpen());
        buffer.rewind();
    }

    @Override
    public PriceBar fromByteBuffer(String symbol) {
        PriceBar bar = new PriceBar(symbol);
        bar.setId(buffer.getLong());
        bar.setDate(PriceBar.extractDateFromId(bar.getId()));
        bar.setTime(PriceBar.extractTimeFromId(bar.getId()));
        bar.setOpen(buffer.getDouble());
        buffer.rewind();
        return bar;
    }

    @Override
    public PriceBar read(RandomAccessFile file, String symbol) throws IOException {
        int nbytes = file.read(buffer.array());
        if (nbytes < getRecordSizeBytes()) {
            return null;
        }

        return fromByteBuffer(symbol);
    }

    @Override
    public long getItemId(PriceBar item) {
        return item.getId();
    }

    @Override
    public long getItemDate(PriceBar item) {
        return  item.getDate();
    }

}

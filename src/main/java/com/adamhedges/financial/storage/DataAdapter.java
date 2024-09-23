package com.adamhedges.financial.storage;

import com.adamhedges.utilities.filesystem.ResourceUtilities;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class DataAdapter<T extends Comparable<T>> {

    private final String rootPath;
    private final String dataPath;
    private final String indexPath;

    public final ByteBuffer buffer;

    public DataAdapter(String rootPath, String datapath, String indexpath) {
        this.rootPath = rootPath;
        this.dataPath = datapath;
        this.indexPath = indexpath;
        this.buffer = ByteBuffer.allocate(getRecordSizeBytes()).order(ByteOrder.LITTLE_ENDIAN);
    }

    public String getDataPath() {
        return ResourceUtilities.getResourceFilePath(rootPath, dataPath);
    }

    public String getDataFilePath(String symbol) {
        String path = getDataPath();
        return ResourceUtilities.getResourceFilePath(path, symbol);
    }

    public String getIndexPath() {
        return ResourceUtilities.getResourceFilePath(rootPath, indexPath);
    }

    public String getIndexFilePath(String symbol) {
        String path = getIndexPath();
        return ResourceUtilities.getResourceFilePath(path, String.format("%s.csv", symbol));
    }

    public abstract int getRecordSizeBytes();
    public abstract int getRecordSizeBytes(int numRecords);
    public abstract void toByteBuffer(T item);
    public abstract T fromByteBuffer(String symbol);
    public abstract T read(RandomAccessFile file, String symbol) throws IOException;

    public abstract long getItemId(T item);
    public abstract long getItemDate(T item);

}

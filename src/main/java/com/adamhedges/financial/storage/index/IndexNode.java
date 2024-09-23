package com.adamhedges.financial.storage.index;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class IndexNode {

    private long date;
    private long offset;
    private int count;

    public static IndexNode fromCsv(String csvLine) {
        String[] fields = csvLine.trim().split(",");
        IndexNode node = new IndexNode();
        node.date = Long.parseLong(fields[0]);
        node.offset = Long.parseLong(fields[1]);
        node.count = Integer.parseInt(fields[2]);
        return node;
    }

    public String toCsv() {
        return String.format("%s,%s,%s", date, offset, count);
    }

    public boolean isAfter(IndexNode other) {
        return this.date > other.date;
    }

    public boolean isBefore(IndexNode other) {
        return this.date < other.date;
    }

    public void updateOffset(int deltabytes) {
        offset += deltabytes;
    }

}

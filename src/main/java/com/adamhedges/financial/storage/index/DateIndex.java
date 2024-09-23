package com.adamhedges.financial.storage.index;

import java.util.TreeMap;

public class DateIndex extends TreeMap<Long, IndexNode> {

    private boolean isDirty = false;

    public void setDirty() {
        this.isDirty = true;
    }

    public void setClean() {
        this.isDirty = false;
    }

    public boolean isDirty() {
        return this.isDirty;
    }

}

package com.adamhedges.financial.storage.index;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDateIndex {

    @Test
    public void TestDateIndex_dirtyFlag() {
        DateIndex index = new DateIndex();
        index.setDirty();
        Assertions.assertTrue(index.isDirty());
        index.setClean();
        Assertions.assertFalse(index.isDirty());
    }

}

package com.adamhedges.financial.storage.exceptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFileStoreIntegrityException {

    @Test
    public void TestFileStoreIntegrityException_message() {
        FileStoreIntegrityException fsiex = new FileStoreIntegrityException("TEST", "Some error message");
        Assertions.assertEquals("[TEST] Some error message", fsiex.getMessage());
    }

}

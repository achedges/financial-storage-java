package com.adamhedges.financial.storage.exceptions;

public class FileStoreIntegrityException extends Exception {

    public FileStoreIntegrityException(String symbol, String message) {
        super(String.format("[%s] %s", symbol, message));
    }

}

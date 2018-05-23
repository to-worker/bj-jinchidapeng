package com.zqykj.streaming.exception;

public class TransformerException extends RuntimeException {

    public TransformerException(String arg0) {
        super(arg0);
    }

    public TransformerException(String arg0, Exception e1) {
        super(arg0, e1);
    }

    private static final long serialVersionUID = 1L;

}
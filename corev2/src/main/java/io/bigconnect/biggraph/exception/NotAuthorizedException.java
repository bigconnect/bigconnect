package io.bigconnect.biggraph.exception;

import io.bigconnect.biggraph.BigGraphException;

public class NotAuthorizedException extends BigGraphException {

    private static final long serialVersionUID = -1407924451828873200L;

    public NotAuthorizedException(String message) {
        super(message);
    }

    public NotAuthorizedException(String message, Object... args) {
        super(message, args);
    }
}

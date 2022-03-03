package org.ekbana.server.Exceptions;

public class InvalidPolicyTypeException extends RuntimeException {

    public InvalidPolicyTypeException(String expecting, String actual) {
        super("Expecting : " +expecting+ " Actual : "+actual);
    }
}

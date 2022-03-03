package org.ekbana.server.Exceptions;

public class PolicyNotFoundException extends RuntimeException {

    public PolicyNotFoundException(String policyName) {
        super(policyName + " not registered in system");
    }
}

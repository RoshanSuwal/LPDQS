package org.ekbana.broker.Policy;

public interface Policy<T> {
    boolean validate(T t);
}

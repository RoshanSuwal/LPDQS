package org.ekbana.minikafka.plugin.policy;

public interface Policy<T> {
    boolean validate(T t);
}

package org.ekbana.minikafka.plugin.policy;

import java.util.Properties;

public interface PolicyFactory<T> {
    String policyName();
    PolicyType policyType();
    Policy<T> buildPolicy(Properties properties);
}

package org.ekbana.minikafka.plugin.policy;

import java.util.Properties;

public interface PolicyFactory<T> {
    String policyName();
    Policy<T> buildPolicy(Properties properties);
}

package org.ekbana.minikafka.plugin;

import org.ekbana.minikafka.plugin.policy.PolicyFactory;

import java.util.Collections;
import java.util.List;

public interface Plugin {

    String pluginName();
    String pluginDescription();
    default List<PolicyFactory<?>> getPolicyFactories(){
        return Collections.emptyList();
    }
}

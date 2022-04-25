package org.ekbana.server;

import org.ekbana.minikafka.plugin.loadbalancer.LoadBalancerFactory;
import org.ekbana.minikafka.plugin.policy.PolicyFactory;
import org.ekbana.minikafka.plugin.policy.PolicyType;
import org.ekbana.minikafka.plugins.DefaultPlugin;
import org.ekbana.plugin.core.PluginLoader;
import org.ekbana.server.Exceptions.InvalidPolicyTypeException;
import org.ekbana.server.Exceptions.PolicyNotFoundException;

import java.io.File;
import java.util.Collections;

public class KafkaLoader {

    private final PluginLoader pluginLoader;

    public KafkaLoader(String pluginDir) {
        this.pluginLoader=new PluginLoader(new File(pluginDir));
    }

    public void load(){
        pluginLoader.loadPlugins();
        pluginLoader.loadBuiltInPlugins(Collections.singletonList(new DefaultPlugin()));
    }

    public void validatePolicy(String policyName, PolicyType policyType){
        final PolicyFactory<?> policyFactory = getPolicyFactory(policyName);
        if (policyFactory==null) throw new PolicyNotFoundException(policyName);
        else if (policyFactory.policyType()!=policyType) throw new InvalidPolicyTypeException(policyFactory.policyType().toString(),policyType.toString());
    }

    public PolicyFactory<?> getPolicyFactory(String policyName){
        return pluginLoader.getPolicyFactory(policyName);
    }

    public LoadBalancerFactory<?,?,?> getLoadBalancerFactory(String loadBalancerName){
        return pluginLoader.getLoadBalancerFactory(loadBalancerName);
    }

}

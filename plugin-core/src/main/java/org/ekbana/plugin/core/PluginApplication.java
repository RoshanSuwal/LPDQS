package org.ekbana.plugin.core;

import org.ekbana.minikafka.plugin.policy.Policy;
import org.ekbana.minikafka.plugin.policy.PolicyFactory;
import org.ekbana.minikafka.plugins.DefaultPlugin;

import java.io.File;
import java.util.Collections;
import java.util.Properties;

public class PluginApplication {

    public static void main(String[] args) {

        String pluginPath = "plugins";

        PluginLoader pluginLoader = new PluginLoader(new File(pluginPath));
        pluginLoader.loadPlugins();

        pluginLoader.loadBuiltInPlugins(Collections.singletonList(
                new DefaultPlugin())
        );

        System.out.println();
        for (PolicyFactory<?> factory: pluginLoader.getPolicyFactories()){
            if (factory==null){
                System.err.println("No factories loaded!");
                continue;
            }
            System.out.println(factory.policyName()+" : "+factory.policyType());
        }

        System.out.println();
        final PolicyFactory<?> policyFactory = pluginLoader.getPolicyFactory("size-based-segment-batch-policy");
        System.out.println(policyFactory.policyName());
        final Policy<?> policy = policyFactory.buildPolicy(new Properties());
        System.out.println("testing");
    }
}

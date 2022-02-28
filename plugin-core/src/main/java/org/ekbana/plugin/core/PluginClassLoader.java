package org.ekbana.plugin.core;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;

public class PluginClassLoader extends URLClassLoader {
    public static final List<String> SHARED_PACKAGES = Arrays.asList(
            "org.ekbana.minikafka.plugin"
    );

    private final ClassLoader parentClassLoader;

    public PluginClassLoader(URL[] urls, ClassLoader parentClassLoader) {
        super(urls, null);
        this.parentClassLoader = parentClassLoader;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        Class<?> loadedClass = findLoadedClass(name);
        if (loadedClass == null) {
            final boolean isSharedClass = SHARED_PACKAGES.stream().allMatch(name::startsWith);
            if (isSharedClass) {
                loadedClass = parentClassLoader.loadClass(name);
            } else {
                loadedClass = super.loadClass(name, resolve);
            }
        }

        if (resolve) resolveClass(loadedClass); //marked as resolve
        return loadedClass;
    }
}

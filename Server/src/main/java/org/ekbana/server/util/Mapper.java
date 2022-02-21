package org.ekbana.server.util;

import java.util.HashMap;
import java.util.Map;

public class Mapper<K,V> {
    private final Map<K,V> map=new HashMap<>();
    public void add(K k,V v){
        map.put(k,v);
    }

    public V get(K k){
        return map.get(k);
    }

    public void delete(K k){
        map.remove(k);
    }

    public boolean has(K k){
        return map.containsKey(k);
    }
}

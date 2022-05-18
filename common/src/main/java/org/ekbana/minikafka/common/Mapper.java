package org.ekbana.minikafka.common;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Mapper<K,V> {
    private final ConcurrentHashMap<K,V> map=new ConcurrentHashMap<>();
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
    public Set<K> getKeys(){return map.keySet();}
    public Collection<V> getValues(){return map.values();}

    public int size(){return map.size();}
    public void forEach(IMapper<K,V> iMapper){
        map.forEach(iMapper::apply);
    }

    public interface IMapper<K,V>{
        void apply(K k,V v);
    }
}

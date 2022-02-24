package org.ekbana.server.util;

import java.util.Arrays;
import java.util.List;

public class Helper {

    @FunctionalInterface
    public interface Apply{
        void apply(int key, Object value);
    }
    public static void mapList(List<?> t, Apply apply){
        for (int i=0;i<t.size();i++){
            apply.apply(i,t.get(i));
        }
    }

    public static void mapArray(Object[] t, Apply apply){
        map(Arrays.stream(t).toList(),apply);
    }

    public static void map(List<?> t, Apply apply){
        for (int i=0;i<t.size();i++){
            apply.apply(i,t.get(i));
        }
    }
}

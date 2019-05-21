package org.apache.spark.ml.Example;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.FlatMapFunction;

public class Tu1 implements FlatMapFunction<String, String> {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private static final Pattern SPACE = Pattern.compile(" ");
    // @Override
    public Iterable<String> call(String s) {
        return Arrays.asList(SPACE.split(s));
    }
}
package com.mware.core.ingest.dataworker;

import com.mware.ge.Property;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class DataWorkerMemoryTracer {
    public static boolean ENABLED = false;
    private static Map<Long, String> LOGS = Collections.synchronizedMap(new LinkedHashMap<>());

    public static void log(String dwClass, String elementId, Property property) {
        if (ENABLED) {
            LOGS.put(System.nanoTime(), String.format("doWork (%s): %s %s", dwClass, elementId, property));
        }
    }

    public static void clear() {
        LOGS.clear();
    }

    public static void print() {
        System.out.println("-------------------------------------------------------------");
        DataWorkerMemoryTracer.LOGS.forEach((k, v) -> System.out.printf("%s %s\n", k, v));
        System.out.println("-------------------------------------------------------------");
    }
}

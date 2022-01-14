package cn.edu.ruc.tsbenchmark.result;

import java.util.LinkedHashMap;

public class Result {
    private static final LinkedHashMap<String, String> resultMap = new LinkedHashMap<>();

    public static synchronized void putResult(String k, String v) {
        resultMap.put(k, v);
    }

    public static void printResult() {
        for (String result : resultMap.keySet()) {
            System.out.println(result + " : " + resultMap.get(result));
        }
    }
}

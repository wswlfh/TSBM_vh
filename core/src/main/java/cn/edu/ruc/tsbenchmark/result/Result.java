package cn.edu.ruc.tsbenchmark.result;

import cn.edu.ruc.tsbenchmark.collection.ProductQueue;
import cn.edu.ruc.tsbenchmark.config.Config;
import cn.edu.ruc.tsbenchmark.schema.MetaDataSchema;

import java.util.LinkedHashMap;

public class Result {
    private static final LinkedHashMap<String, String> resultMap = new LinkedHashMap<>();
    private static final Config config = Config.getInstance();
    private static final MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();

    public static void before(){
        Result.putResult("PRODUCER_NUMBER",config.getPRODUCER_NUMBER()+"");
        Result.putResult("CONSUMER_NUMBER",config.getCONSUMER_NUMBER()+"");
        Result.putResult("Number of time series", config.getTAG_TOTAL() + "");
        Result.putResult("Number of time tag", metaDataSchema.getTagNames().length + "");
        Result.putResult("Number of time field", metaDataSchema.getFieldSchema().length + "");
        Result.putResult("Number of theoretical insert records", config.getTHEORETICAL_SIZE() + "");
    }
    public static synchronized void putResult(String k, String v) {
        resultMap.put(k, v);
    }

    public static void printResult() {
        for (String result : resultMap.keySet()) {
            System.out.println(result + " : " + resultMap.get(result));
        }
    }
}

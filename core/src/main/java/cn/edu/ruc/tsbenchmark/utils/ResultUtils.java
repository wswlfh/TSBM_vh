package cn.edu.ruc.tsbenchmark.utils;

import cn.edu.ruc.tsbenchmark.config.Config;
import cn.edu.ruc.tsbenchmark.schema.MetaDataSchema;

import java.util.LinkedHashMap;

public class ResultUtils {
    private static final LinkedHashMap<String, String> resultMap = new LinkedHashMap<>();
    private static final Config config = Config.getInstance();
    private static final MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();

    public static void before(){
        ResultUtils.putResult("PRODUCER_NUMBER",config.getPRODUCER_NUMBER()+"");
        ResultUtils.putResult("CONSUMER_NUMBER",config.getCONSUMER_NUMBER()+"");
        ResultUtils.putResult("Number of time series", config.getTAG_TOTAL() + "");
        ResultUtils.putResult("Number of time tag", metaDataSchema.getTagNames().length + "");
        ResultUtils.putResult("Number of time field", metaDataSchema.getFieldSchema().length + "");
        ResultUtils.putResult("Number of theoretical insert records", config.getTHEORETICAL_SIZE() + "");
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

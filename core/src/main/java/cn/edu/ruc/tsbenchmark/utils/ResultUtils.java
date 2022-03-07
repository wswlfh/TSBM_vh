package cn.edu.ruc.tsbenchmark.utils;

import cn.edu.ruc.tsbenchmark.config.Config;
import cn.edu.ruc.tsbenchmark.schema.MetaDataSchema;

import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicLongArray;

public class ResultUtils {
    private static final LinkedHashMap<String, String> resultMap = new LinkedHashMap<>();
    private static final Config config = Config.getInstance();
    private static final MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();

    private static final AtomicLongArray costTimes = new AtomicLongArray(config.getCONSUMER_NUMBER());
    private static final AtomicLongArray records = new AtomicLongArray(config.getCONSUMER_NUMBER());


    public static void before() {
        putResult("PRODUCER_NUMBER", config.getPRODUCER_NUMBER() + "");
        putResult("CONSUMER_NUMBER", config.getCONSUMER_NUMBER() + "");
        putResult("Number of time series", config.getTAG_TOTAL() + "");
        putResult("Number of time tag", metaDataSchema.getTagNames().length + "");
        putResult("Number of time field", metaDataSchema.getFieldSchema().length + "");
        putResult("Number of theoretical insert records", config.getTHEORETICAL_SIZE() + "");
    }

    public static void after() {
        //Calculate average write speed
        double speed = 0;
        for (int i = 0; i < config.getCONSUMER_NUMBER(); i++)
            speed += (double) records.get(i) / costTimes.get(i) * 1000;
        putResult("write record speed", speed / config.getCONSUMER_NUMBER() + " record/s");
        putResult("write point speed", speed / config.getCONSUMER_NUMBER() *
                (metaDataSchema.getFieldSchema().length) + " point/s");

    }

    public static void addCostTime(int id, int delta) {
        costTimes.addAndGet(id, delta);
    }

    public static void addRecord(int id, int delta) {
        records.addAndGet(id, delta);
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

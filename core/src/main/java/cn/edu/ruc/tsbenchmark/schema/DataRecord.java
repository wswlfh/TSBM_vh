package cn.edu.ruc.tsbenchmark.schema;

import java.util.LinkedHashMap;

public class DataRecord {

    private final int ProduceId;

    private final long timeStamp;

    private final String tagString;
    private String fieldString;


    public DataRecord(int produceId, long timeStamp, String tagString, String fieldSchema, Object[] fieldValues) {
        ProduceId = produceId;
        this.timeStamp = timeStamp;
        this.tagString = tagString;

        this.fieldString = String.format(fieldSchema, fieldValues);
    }


    public int getProduceId() {
        return ProduceId;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public String getTagString() {
        return tagString;
    }


}

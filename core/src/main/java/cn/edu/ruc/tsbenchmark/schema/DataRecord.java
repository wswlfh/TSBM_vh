package cn.edu.ruc.tsbenchmark.schema;

import java.util.LinkedHashMap;

public class DataRecord {

    private final int ProduceId;
    private final long timeStamp;
    private final String tagString;
    private final String fieldString;


    public DataRecord(int produceId, long timeStamp, String tagString, String fieldString) {
        ProduceId = produceId;
        this.timeStamp = timeStamp;
        this.tagString = tagString;
        this.fieldString = fieldString;
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

    public String getFieldString() {
        return fieldString;
    }


}

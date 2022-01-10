package cn.edu.ruc.tsbenchmark.schema;

import java.util.LinkedHashMap;

public class DataRecord {
    private final int ProduceId;

    private final long timeStamp;

    private final String tagString;

    private final LinkedHashMap<String, String> fieldValues;


    public DataRecord(int produceId, long timeStamp, String tagString, LinkedHashMap<String, String> fieldValues) {
        ProduceId = produceId;
        this.timeStamp = timeStamp;
        this.tagString = tagString;
        this.fieldValues = fieldValues;
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

    public LinkedHashMap<String, String> getFieldValues() {
        return fieldValues;
    }


}

package cn.edu.ruc.tsbenchmark.schema;

import java.util.LinkedHashMap;

public class DataRecord {

    private final long timeStamp;
    private final String tagString;
    //private final String fieldString;
    private final Object[] fieldsValue;



    public DataRecord(long timeStamp, String tagString, Object[] fieldsValue) {
        this.timeStamp = timeStamp;
        this.tagString = tagString;
        this.fieldsValue = fieldsValue;
        //this.fieldString = fieldString;
    }



    public long getTimeStamp() {
        return timeStamp;
    }

    public String getTagString() {
        return tagString;
    }

    public Object[] getFieldsValue() {
        return fieldsValue;
    }
}

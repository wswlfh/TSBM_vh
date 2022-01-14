package cn.edu.ruc.tsbenchmark.schema;

import java.util.LinkedList;
import java.util.List;

public class Batch {
    private final int ProduceId;
    private final boolean isEmpty;
    private final long timeStamp;

    private final LinkedList<DataRecord> recordList;


    public Batch(int produceId, long timeStamp, LinkedList<DataRecord> recordList) {
        ProduceId = produceId;
        this.timeStamp = timeStamp;
        //this.recordList = new LinkedList<>(recordList);
        this.recordList = recordList;
        this.isEmpty = false;
    }

    // make a empty batch
    public Batch(boolean isEmpty) {
        assert isEmpty;
        this.ProduceId = -1;
        this.timeStamp = -1;
        this.recordList = null;

        this.isEmpty = true;
    }

    public int getProduceId() {
        return ProduceId;
    }

    public boolean isEmpty() {
        return isEmpty;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public LinkedList<DataRecord> getRecordList() {
        return recordList;
    }
}

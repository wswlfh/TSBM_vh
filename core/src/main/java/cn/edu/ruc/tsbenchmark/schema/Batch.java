package cn.edu.ruc.tsbenchmark.schema;

import java.util.List;

public class Batch {
    private final int ProduceId;


    private final boolean isEmpty;
    private final long timeStamp;

    private final List<DataRecord> recordList;


    public Batch(int produceId, long timeStamp, List<DataRecord> recordList) {
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

    public List<DataRecord> getRecordList() {
        return recordList;
    }
}

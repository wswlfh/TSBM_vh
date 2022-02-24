package cn.edu.ruc.tsbenchmark.schema;

import java.util.LinkedList;

public class Batch {
    private final int id;
    private final int producerId;
    private final long startIndex;
    private final long endIndex;

    private final boolean isEmpty;

    private LinkedList<DataRecord> recordList;


    public Batch(int producerId, int id, long startIndex, long endIndex) {
        this.producerId = producerId;
        this.id = id;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.isEmpty = false;

    }

    public void addRecordList(LinkedList<DataRecord> recordList) {
        this.recordList = recordList;
    }

    // make a empty batch
    public Batch(boolean isEmpty) {
        assert isEmpty;
        this.id = -1;
        this.producerId = -1;
        this.startIndex = -1;
        this.endIndex = -1;
        this.recordList = null;

        this.isEmpty = true;
    }

    public int getId() {
        return id;
    }

    public boolean isEmpty() {
        return isEmpty;
    }


    public int getProducerId() {
        return producerId;
    }

    public LinkedList<DataRecord> getRecordList() {
        return recordList;
    }
}

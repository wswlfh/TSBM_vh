package cn.edu.ruc.tsbenchmark.client.product;

import cn.edu.ruc.tsbenchmark.client.Client;
import cn.edu.ruc.tsbenchmark.schema.Batch;
import cn.edu.ruc.tsbenchmark.schema.DataRecord;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;


public class ProducerClient extends Client {

    public ProducerClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
        super(id, countDownLatch, barrier);
    }

    @Override
    protected void execute() {
        ArrayList<Long> timestampList = metaDataSchema.getTimestampList();
        ArrayList<String> tagsList = metaDataSchema.getTagsList();
        ConcurrentHashMap<Integer, Deque<Batch>> productMean = metaDataSchema.getProductMean();
        Deque<Batch> batchDeque = productMean.get(this.id);
        while (!batchDeque.isEmpty()) {
            Batch batch = batchDeque.pop();
            LinkedList<DataRecord> recordList = new LinkedList<>();
            int timestampStartIndex = (int) (batch.getStartIndex() / tagsList.size()) % timestampList.size();
            int tagStartIndex = (int) batch.getStartIndex() % tagsList.size();
            int timestampEndIndex = (int) (batch.getEndIndex() / tagsList.size()) % timestampList.size();
            int tagEndIndex = (int) batch.getEndIndex() % tagsList.size();
            for (int i = timestampStartIndex; i <= timestampEndIndex; i++) {
                Long timestamp = timestampList.get(i);
                for (int j = tagStartIndex; j <= tagEndIndex; j++) {
                    String tagString = tagsList.get(j);
                    recordList.add(new DataRecord(timestamp, tagString, valuesMap.get(i + j)));
                }
            }
            batch.addRecordList(recordList);
            productQueue.put(batch);
        }
        Status.setProductStatusById(this.id);
    }

//    protected void execute() {
//
//        ConcurrentHashMap<Integer, Deque<String>> tagsMap = null;
//        Deque<String> queue = tagsMap.get(this.id);
//        LinkedList<DataRecord> lists = new LinkedList<>();
//        long timeStamp = 0;
//        while (!queue.isEmpty()) {
//            timeStamp = config.getTIME_START() - config.getTIME_INTERVAL();
//            String tagString = queue.pop();
//            for (int i = 0; i < config.getLOOP(); i++) {
//                timeStamp = Math.min(config.getTIME_END(), timeStamp + config.getTIME_INTERVAL());//防止取不到end
//                DataRecord record = new DataRecord(this.id, timeStamp, tagString, valuesMap.get(i));
//                lists.add(record);
//                if (lists.size() == config.getBATCH_SIZE()) {
//                    productQueue.put(new Batch(this.id, timeStamp, new LinkedList<>(lists)));
//                    lists.clear();
//                }
//            }
//        }
//        if (lists.size() != 0)
//            productQueue.put(new Batch(this.id, timeStamp == 0 ? config.getTIME_END() : timeStamp, lists));
//        ProductStatus.setStatusById(this.id);
//    }
}

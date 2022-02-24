package cn.edu.ruc.tsbenchmark.client.product;

import cn.edu.ruc.tsbenchmark.client.Client;
import cn.edu.ruc.tsbenchmark.schema.Batch;
import cn.edu.ruc.tsbenchmark.schema.DataRecord;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;


public class ProducerClient extends Client {

    public ProducerClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
        super(id, countDownLatch, barrier);
    }

    @Override
    protected void execute() {

        Deque<Long> timestampQueue = new LinkedList<>();
        ArrayList<String> tagsList = metaDataSchema.getTagsList();
        LinkedList<DataRecord> recordLists = new LinkedList<>();
        int batchId = 0;
        boolean isLast = false;
        while (!timestampQueue.isEmpty()) {
            Long timestamp = timestampQueue.pop();
            for (int i = 0; i < tagsList.size(); i++) {
                String tagString = tagsList.get(i);
                DataRecord record = new DataRecord(timestamp, tagString, valuesMap.get(i));
                recordLists.add(record);
                if (recordLists.size() == config.getBATCH_SIZE()) {
                    if (timestampQueue.isEmpty() && i == tagsList.size() - 1)
                        isLast = true;
                    //productQueue.put(new Batch(this.id, batchId++, new LinkedList<>(recordLists), endIndex, isLast));
                    recordLists.clear();
                }
            }
        }
//        if (recordLists.size() != 0)
//            productQueue.put(new Batch(this.id, batchId, new LinkedList<>(recordLists), endIndex, true));
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        ProductStatus.setStatusById(this.id);
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

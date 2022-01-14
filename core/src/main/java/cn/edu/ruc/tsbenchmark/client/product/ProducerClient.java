package cn.edu.ruc.tsbenchmark.client.product;

import cn.edu.ruc.tsbenchmark.client.Client;
import cn.edu.ruc.tsbenchmark.schema.Batch;
import cn.edu.ruc.tsbenchmark.schema.DataRecord;

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
        ConcurrentHashMap<Integer, Deque<String>> tagsMap = metaDataSchema.getTagsMap();
        Deque<String> queue = tagsMap.get(this.id);
        LinkedList<DataRecord> lists = new LinkedList<>();
        long timeStamp = 0;
        while (!queue.isEmpty()) {
            timeStamp = config.getTIME_START() - config.getTIME_INTERVAL();
            String tagString = queue.pop();
            for (int i = 0; i < config.getLOOP(); i++) {
                timeStamp = Math.min(config.getTIME_END(), timeStamp + config.getTIME_INTERVAL());//防止取不到end
                DataRecord record = new DataRecord(this.id, timeStamp, tagString, valuesMap.get(i));
                lists.add(record);
                if (lists.size() == config.getBATCH_SIZE()) {
                    productQueue.put(new Batch(this.id, timeStamp, new LinkedList<>(lists)));
                    lists.clear();
                }
            }
        }
        if (lists.size() != 0)
            productQueue.put(new Batch(this.id, timeStamp == 0 ? config.getTIME_END() : timeStamp, lists));
        ProductStatus.setStatusById(this.id);
    }
}

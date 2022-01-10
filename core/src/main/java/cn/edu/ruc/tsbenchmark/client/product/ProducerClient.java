package cn.edu.ruc.tsbenchmark.client.product;

import cn.edu.ruc.tsbenchmark.client.Client;
import cn.edu.ruc.tsbenchmark.config.Config;
import cn.edu.ruc.tsbenchmark.queue.ProductQueue;
import cn.edu.ruc.tsbenchmark.schema.Batch;
import cn.edu.ruc.tsbenchmark.schema.DataRecord;
import cn.edu.ruc.tsbenchmark.schema.MetaDataSchema;
import cn.edu.ruc.tsbenchmark.utils.ValueUtils;

import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;


public class ProducerClient extends Client {
    private static final Config config = Config.getInstance();
    private static final MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();
    private static final ProductQueue productQueue = ProductQueue.getInstance();

    public ProducerClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
        super(id, countDownLatch, barrier);
    }

    @Override
    protected void execute() {
        ConcurrentHashMap<Integer, Deque<String>> tagsMap = metaDataSchema.getTagsMap();
        Deque<String> queue = tagsMap.get(this.id);
        String fieldSchema = metaDataSchema.getFieldSchema();
        LinkedList<DataRecord> lists = new LinkedList<>();
        long timeStamp = timeStamp = config.getTIME_START() - config.getTIME_INTERVAL();
        //生成随机值数组
        ValueUtils.genRandom();
        while (!queue.isEmpty()) {
            //System.out.println(Thread.currentThread().getName() + " producer");
            String tagString = queue.pop();
            for (int i = 0; i < config.getLOOP(); i++) {
                timeStamp = Math.min(config.getTIME_END(), timeStamp + config.getTIME_INTERVAL());//防止取不到end
                DataRecord record = new DataRecord(this.id, timeStamp, tagString, fieldSchema, ValueUtils.getRandomV(i));

            lists.add(record);

            if (lists.size() == config.getBATCH_SIZE()) {
                productQueue.put(new Batch(this.id, timeStamp, new LinkedList<>(lists)));
                lists.clear();
            }
        }
    }

        if(lists.size()!=0)
                productQueue.put(new

    Batch(this.id, timeStamp ==0?config.getTIME_END() :timeStamp,lists));
        ProductStatus.setStatusById(this.id);
}
}

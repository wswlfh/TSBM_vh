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

        LinkedList<DataRecord> lists = new LinkedList<>();
        long timeStamp = 0;
        while (!queue.isEmpty()) {
            //System.out.println(Thread.currentThread().getName() + " producer");
            String tagString = queue.pop();
            LinkedHashMap<String, String> fieldValues = ValueUtils.getFieldValues(metaDataSchema.getFieldTypes());

            for (long start = config.getTIME_START() - config.getTIME_INTERVAL();
                 start <= config.getTIME_END();
                 start += config.getTIME_INTERVAL()) {
                timeStamp = Math.min(config.getTIME_END(), start + config.getTIME_INTERVAL());//防止取不到end
                //构造batch

                DataRecord record = new DataRecord(this.id, timeStamp, tagString, fieldValues);
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

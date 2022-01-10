package cn.edu.ruc.tsbenchmark.client.consumer;

import cn.edu.ruc.tsbenchmark.client.Client;
import cn.edu.ruc.tsbenchmark.config.Config;
import cn.edu.ruc.tsbenchmark.queue.ProductQueue;
import cn.edu.ruc.tsbenchmark.schema.Batch;
import cn.edu.ruc.tsbenchmark.schema.MetaDataSchema;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public class ConsumerClient extends Client {
    private static final Config config = Config.getInstance();
    private static final MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();
    private static final ProductQueue productQueue = ProductQueue.getInstance();

    public ConsumerClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
        super(id, countDownLatch, barrier);
    }

    @Override
    protected void execute() {
        while (true) {
            try {
                //System.out.println(Thread.currentThread().getName()+" consumer");
                Batch batch = productQueue.take();
                if (batch.isEmpty()) break;
                insertRecords(batch);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void insertRecords(Batch batch) {
        metaDataSchema.size.addAndGet(batch.getRecordList().size());
    }


}


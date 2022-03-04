package cn.edu.ruc.tsbenchmark.client;

import cn.edu.ruc.tsbenchmark.queue.ProductQueue;
import cn.edu.ruc.tsbenchmark.config.Config;
import cn.edu.ruc.tsbenchmark.utils.ValuesUtils;
import cn.edu.ruc.tsbenchmark.schema.MetaDataSchema;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public abstract class Client implements Runnable {
    protected static final Config config = Config.getInstance();
    protected static final MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();
    protected static final ProductQueue productQueue = ProductQueue.getInstance();
    protected static final ValuesUtils valuesMap = ValuesUtils.getInstance();

    public final int id;
    private final CountDownLatch countDownLatch;
    private final CyclicBarrier barrier;


    public Client(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
        this.id = id;
        this.countDownLatch = countDownLatch;
        this.barrier = barrier;
    }


    @Override
    public void run() {
        try {
            barrier.await();
            execute();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            countDownLatch.countDown();
        }
    }

    protected abstract void execute();


}

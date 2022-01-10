package cn.edu.ruc.tsbenchmark.queue;

import cn.edu.ruc.tsbenchmark.client.product.ProductStatus;
import cn.edu.ruc.tsbenchmark.config.Config;
import cn.edu.ruc.tsbenchmark.schema.Batch;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ProductQueue extends PriorityBlockingQueue<Batch> {
    private static final Config config = Config.getInstance();

    private final ReentrantLock putLock = new ReentrantLock();
    private final Condition notFull = putLock.newCondition();

    private final ReentrantLock takeLock = new ReentrantLock();
    private final Condition notEmpty = takeLock.newCondition();

    private final AtomicInteger count = new AtomicInteger();

    private final int capacity;

    //private final static int factor = 10000;


    public ProductQueue() {
        super((int) config.getREASONABLE_CAPACITY(), (o1, o2) -> {
            if (o1.getProduceId() == o2.getProduceId())
                return o1.getTimeStamp() < o2.getTimeStamp() ? -1 : 1;
            return o1.getProduceId() < o2.getProduceId() ? -1 : 1;
        });

        //不减1也可以，可自定义 super()创造的是优先队列的最大容量，this.capacity指 放满(put操作)导致阻塞的临界值
        this.capacity = (int) config.getREASONABLE_CAPACITY() - 1;

    }

    private static class ProductQueueHolder {

        private static final ProductQueue INSTANCE = new ProductQueue();
    }

    public static ProductQueue getInstance() {
        return ProductQueueHolder.INSTANCE;
    }

    @Override
    public void put(Batch batch) {
        int c = -1;
        final ReentrantLock putLock = this.putLock;
        final AtomicInteger count = this.count;
        try {
            putLock.lockInterruptibly();
            while (count.get() == capacity) {
                notFull.await();
            }
            super.offer(batch);
            c = count.getAndIncrement();
            if (c + 1 < capacity)
                notFull.signalAll();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            putLock.unlock();
        }
        if (c == 0)
            signalNotEmpty();
    }

    @Override
    public Batch take() throws InterruptedException {
        Batch x;
        int c = -1;
        final AtomicInteger count = this.count;
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lockInterruptibly();
        try {
            while (count.get() == 0) {
                if (ProductStatus.isProductDone())
                    return new Batch(true);
                notEmpty.await();
            }
            x = super.take();
            c = count.getAndDecrement();
            if (c > 1)
                notEmpty.signalAll();
        } finally {
            takeLock.unlock();
        }
        if (c == capacity)
            signalNotFull();
        return x;
    }

    private void signalNotFull() {
        final ReentrantLock putLock = this.putLock;
        putLock.lock();
        try {
            //notFull.signal();
            notFull.signalAll();
        } finally {
            putLock.unlock();
        }
    }

    private void signalNotEmpty() {
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        try {
            notEmpty.signalAll();
        } finally {
            takeLock.unlock();
        }
    }
}

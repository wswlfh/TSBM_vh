package cn.edu.ruc.tsbenchmark.collection;

import cn.edu.ruc.tsbenchmark.client.product.ProductStatus;
import cn.edu.ruc.tsbenchmark.config.Config;
import cn.edu.ruc.tsbenchmark.schema.Batch;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ProductQueue extends PriorityBlockingQueue<Batch> {
    private static final Config config = Config.getInstance();
    private final ConcurrentHashMap<Integer, List<Batch>> batchMap = new ConcurrentHashMap<>();

    private final ReentrantLock putLock = new ReentrantLock();
    private final Condition notFull = putLock.newCondition();

    private final ReentrantLock takeLock = new ReentrantLock();
    private final Condition notEmpty = takeLock.newCondition();

    private final AtomicInteger count = new AtomicInteger();

    private final AtomicBoolean first = new AtomicBoolean(false);
    private final int capacity;

    //private final static int factor = 10000;


    private ProductQueue() {
        super((int) config.getREASONABLE_CAPACITY(), (o1, o2) -> {
            if (o1.getProducerId() == o2.getProducerId())
                return o1.getId() < o2.getId() ? -1 : 1;
            return o1.getProducerId() < o2.getProducerId() ? -1 : 1;
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
            //将每个batch存入map中便于之后查找
            List<Batch> list = batchMap.getOrDefault(batch.getProducerId(), new LinkedList<>());
            list.add(batch);
            batchMap.put(batch.getProducerId(), list);

            //存入队列
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


    public Batch take() throws InterruptedException {
        Batch x;
        int c = -1;
        final AtomicInteger count = this.count;
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lockInterruptibly();
        try {
            //确保第一个元素是0 0
            if (!first.get()) {
                while (!first.get()) {
                    if (peek() != null){
                        Batch peek = peek();
                        if (peek.getProducerId() == 0 && peek.getId() == 0)
                            break;
                    }
                    notEmpty.await(100, TimeUnit.MICROSECONDS);
                }
                first.set(true);
            }

            while (count.get() == 0) {
                if (ProductStatus.isProductDone())
                    return new Batch(true);
                notEmpty.await(100, TimeUnit.MICROSECONDS);
            }
            x = super.poll();
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

    public Batch getBatchByIds(int producerId, int batchId) {
        assert batchMap.containsKey(producerId);
        List<Batch> batchList = batchMap.get(producerId);
        assert batchId >= 0 && batchList.size() > batchId;
        return batchList.get(batchId);
    }
}

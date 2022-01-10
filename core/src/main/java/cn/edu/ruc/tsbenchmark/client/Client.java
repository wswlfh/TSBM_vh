package cn.edu.ruc.tsbenchmark.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public abstract class Client implements Runnable {
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

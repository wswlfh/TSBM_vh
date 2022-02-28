package cn.edu.ruc.tsbenchmark.client.consumer;

import cn.edu.ruc.tsbenchmark.adapter.Adapter;
import cn.edu.ruc.tsbenchmark.adapter.InfluxDBAdapter;
import cn.edu.ruc.tsbenchmark.client.Client;
import cn.edu.ruc.tsbenchmark.result.Result;
import cn.edu.ruc.tsbenchmark.schema.Batch;

import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

public class ConsumerClient extends Client {

    private final Adapter adapter;
    private final AtomicLong costTime = new AtomicLong(0);
    private static final AtomicLong recordNum = new AtomicLong(0);
    private static final LinkedList<Long> progressList = new LinkedList<>();

    public ConsumerClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
        super(id, countDownLatch, barrier);
        adapter = new InfluxDBAdapter();
        for (long i = 0; i < 100; i++)
            progressList.add(i);
    }

    @Override
    protected void execute() {
        while (true) {
//            try {
//                Batch batch = productQueue.take();
//                if (batch.isEmpty()) break;
//                System.out.println(batch.getProducerId() + " " + batch.getId());
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }

            try {
                //System.out.println(Thread.currentThread().getName()+" consumer");
                Batch batch = productQueue.take();
                if (batch.isEmpty()) {
                    adapter.closeConnection();
                    writeResult();
                    System.out.println("the whole test is done!");
                    break;
                }
                //print progress
                long progress = 100 * recordNum.addAndGet(batch.getRecordList().size()) / config.getTHEORETICAL_SIZE();
                if (progressList.getFirst() == progress) {
                    progressList.removeFirst();
                    System.out.println("the whole test is " + progress + "%");
                }

                long time = adapter.insertData(batch);
                costTime.addAndGet(time);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void writeResult() {
        Result.putResult("cost time", costTime.get() + " ms");
        //Result.putResult("Number of records actually inserted", adapter.getCount() + "");
        Result.putResult("write record speed", recordNum.get() * 1000.0 / costTime.get() + " record/s");
        Result.putResult("write point speed", recordNum.get() * 1000.0 / costTime.get() *
                (metaDataSchema.getFieldSchema().length) + " point/s");

    }


}


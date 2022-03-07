package cn.edu.ruc.tsbenchmark.client.consumer;

import cn.edu.ruc.tsbenchmark.adapter.Adapter;
import cn.edu.ruc.tsbenchmark.adapter.InfluxDBAdapter;
import cn.edu.ruc.tsbenchmark.client.Client;
import cn.edu.ruc.tsbenchmark.schema.Batch;
import cn.edu.ruc.tsbenchmark.utils.ResultUtils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public class ConsumerClient extends Client {

    private final Adapter adapter;


    private static final boolean[] progress = new boolean[101];

    public ConsumerClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
        super(id, countDownLatch, barrier);
        adapter = new InfluxDBAdapter();
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
                    break;
                }

                //print progress
                int i = 100 * batch.getId() / metaDataSchema.getBatchTotal();
                if (!progress[i]) {
                    System.out.println("the whole test is " + i + "%");
                    progress[i] = true;
                }

                ResultUtils.addRecord(id, batch.getRecordList().size());
                int time = (int) adapter.insert(batch);
                ResultUtils.addCostTime(id, time);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}


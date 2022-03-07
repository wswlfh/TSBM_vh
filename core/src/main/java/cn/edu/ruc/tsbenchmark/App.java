package cn.edu.ruc.tsbenchmark;

import cn.edu.ruc.tsbenchmark.client.Client;
import cn.edu.ruc.tsbenchmark.client.consumer.ConsumerClient;
import cn.edu.ruc.tsbenchmark.client.product.ProducerClient;
import cn.edu.ruc.tsbenchmark.config.Config;
import cn.edu.ruc.tsbenchmark.utils.ResultUtils;

import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class App {
    private static final Config config = Config.getInstance();

    private static void start() {
        //before
        ResultUtils.before();

        //Thread init
        ExecutorService executorService = Executors.newFixedThreadPool(
                config.getPRODUCER_NUMBER() + config.getCONSUMER_NUMBER());
        CountDownLatch downLatch = new CountDownLatch(config.getPRODUCER_NUMBER() + config.getCONSUMER_NUMBER());
        CyclicBarrier barrier = new CyclicBarrier(config.getPRODUCER_NUMBER() + config.getCONSUMER_NUMBER());
        LinkedList<Client> clients = new LinkedList<>();
        for (int i = 0; i < config.getPRODUCER_NUMBER(); i++) {
            Client client = new ProducerClient(i, downLatch, barrier);
            clients.add(client);
        }
        for (int i = 0; i < config.getCONSUMER_NUMBER(); i++) {
            Client client = new ConsumerClient(i, downLatch, barrier);
            clients.add(client);
        }

        long start = System.currentTimeMillis();
        for (Client client : clients) {
            executorService.submit(client);
        }

        executorService.shutdown();
        try {
            downLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        long end = System.currentTimeMillis();
        ResultUtils.putResult("The whole test process takes ", (end - start) / 1000 + "s");
        ResultUtils.after();
    }


    public static void main(String[] args) {
        start();
        ResultUtils.printResult();
        System.exit(0);
    }
}

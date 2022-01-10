package cn.edu.ruc.tsbenchmark.client.product;

import cn.edu.ruc.tsbenchmark.config.Config;

public class ProductStatus {
    private static final Config config = Config.getInstance();

    private static boolean[] status = new boolean[config.getPRODUCER_NUMBER()];


    protected static synchronized void setStatusById(int id) {
        assert id >= 0 && id < config.getPRODUCER_NUMBER();
        status[id] = true;
    }

    public static synchronized boolean isProductDone() {
        for (int i = 0; i < status.length; i++) {
            if (!status[i])
                return false;
        }
        return true;
    }
}

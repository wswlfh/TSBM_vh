package cn.edu.ruc.tsbenchmark.client.product;

import cn.edu.ruc.tsbenchmark.config.Config;
import cn.edu.ruc.tsbenchmark.schema.MetaDataSchema;

public class Status {
    private static final Config config = Config.getInstance();

    private static final boolean[] productStatus = new boolean[config.getPRODUCER_NUMBER()];


    protected static synchronized void setProductStatusById(int id) {
        assert id >= 0 && id < config.getPRODUCER_NUMBER();
        productStatus[id] = true;
    }

    public static synchronized boolean isProductDone() {
        for (boolean b : productStatus)
            if (!b) return false;

        return true;
    }

}

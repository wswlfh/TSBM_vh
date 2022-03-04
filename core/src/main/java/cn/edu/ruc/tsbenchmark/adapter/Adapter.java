package cn.edu.ruc.tsbenchmark.adapter;

import cn.edu.ruc.tsbenchmark.config.Config;
import cn.edu.ruc.tsbenchmark.schema.Batch;
import cn.edu.ruc.tsbenchmark.schema.MetaDataSchema;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;

public abstract class Adapter {
    /**
     * Adapter for database IO.
     *
     * @author Shengwei Huang, wswlfh@163.com
     */

    protected static final Config config = Config.getInstance();
    protected static final MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();

    public Adapter() {
        initConnect();
    }

    /**
     * All configuration information about the database should be put into this map.
     *
     * Note:Please define the configuration information in the method initConfiguration().
     *
     * @see #initConfiguration()
     */
//    public final HashMap<String, String> dbConfig;
//
//    Adapter() {
//        this.dbConfig = initConfiguration();
//    }

    /**
     * Return a corresponding hashmap to initialize  dbConfig in the constructor.
     *
     * @return A hashmap containing the parameters required to configure the database.
     */
    //protected abstract HashMap<String, String> initConfiguration();

    /**
     * Initialize database connection
     */
    public abstract void initConnect();

    /**
     * Insert a batch of data and calculate the cost time
     *
     * @return timeout, if request failed, please return -1;
     */
    protected abstract long insertData(Batch data);

    public abstract void closeConnection();


    public long insert(Batch data) {
        int id = data.getId();
        ConcurrentHashMap<Integer, Boolean> batchStatus = metaDataSchema.getBatchStatus();
        //保证写入顺序
        if (id != 0) {
            while (true) {
                if (batchStatus.get(id - 1)) break;
            }
        }
        FutureTask<Long> futureTask = new FutureTask<>(() -> insertData(data));
        new Thread(futureTask).start();
        batchStatus.put(id, true);
        long res = 0;
        try {
            res = futureTask.get();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Insert error!");
            System.exit(-1);
        }
        return res;
    }


}

package cn.edu.ruc.tsbenchmark.adapter;
import cn.edu.ruc.tsbenchmark.config.Config;
import cn.edu.ruc.tsbenchmark.schema.Batch;

public interface BaseAdapter {
    /**
     * Adapter for database IO.
     *
     * @author Shengwei Huang, wswlfh@163.com
     *
     */

    public static final Config config = Config.getInstance();
    /**
     * Initialize database connection
     */
    public void initConnect();



    /**
     * Insert a batch of data and calculate the cost time
     *
     * @return timeout, if request failed, please return -1;
     */
    public long insertData(Batch data);


    public void closeConnection();

//    public long query1(long start, long end);
//
//    public long query2(long start, long end, double value);
//
//    public long query3(long start, long end);
//
//    public long query4(long start, long end);
//
//    public long query5(long start, long end);
}


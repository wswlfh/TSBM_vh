package cn.edu.ruc.tsbenchmark.adapter;


/**
 * adapter base interface
 * 适配器基础类
 */
public interface BaseAdapter {
    public void initConnect(String ip, String port, String user, String password);
    /**
     * @return timeout ,if request failed, please return -1;
     */
    public long insertData(String data);

    public long query1(long start, long end);

    public long query2(long start, long end, double value);

    public long query3(long start, long end);

    public long query4(long start, long end);

    public long query5(long start, long end);
}


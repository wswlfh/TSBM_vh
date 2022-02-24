package test;

import cn.edu.ruc.tsbenchmark.config.Config;

import java.util.HashMap;

public interface AdapterInterface {
    public HashMap<Object, Object> config = new HashMap<>();

    public HashMap<Object, Object> initConfig();

    public void initConnect(HashMap<Object, Object> map);

    /**
     * @return timeout ,if request failed, please return -1;
     */
    public long insertData(String data);
}

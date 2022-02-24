package test;

import java.util.HashMap;
import java.util.Map;

public abstract class AdapterAbstract {
    public final HashMap<String, String> config;

    AdapterAbstract() {
        this.config = initConfiguration();
    }

    protected abstract HashMap<String, String> initConfiguration();

    public abstract void initConnect();

    /**
     * @return timeout ,if request failed, please return -1;
     */
    public abstract long insertData(String data);
}

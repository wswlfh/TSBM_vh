package cn.edu.ruc.tsbenchmark.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {
    private int PRODUCER_NUMBER;
    private int CONSUMER_NUMBER;

    private int BATCH_SIZE;

    private int DEVICE_NUMBER;

    private int TAG_TOTAL;


    private String TAG_NAME;
    private String TAG_PROPORTION;

    private int FIELD_NUMBER;
    private String FIELD_PROPORTION;

    private long TIME_INTERVAL;


    private long TIME_START;

    private long TIME_END;

    //生产线能存放的合理容量
    private long REASONABLE_CAPACITY;

    Config() {
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream("conf/config.properties");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        Properties properties = new Properties();
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            System.out.println("load properties failed!");
            e.printStackTrace();
        }

        try {
            this.PRODUCER_NUMBER = Integer.parseInt(properties.getProperty("PRODUCER_NUMBER", ""));
            this.CONSUMER_NUMBER = Integer.parseInt(properties.getProperty("CONSUMER_NUMBER", ""));

            this.BATCH_SIZE = Integer.parseInt(properties.getProperty("BATCH_SIZE", ""));
            this.DEVICE_NUMBER = Integer.parseInt(properties.getProperty("DEVICE_NUMBER", ""));
            this.TAG_TOTAL = Integer.parseInt(properties.getProperty("TAG_TOTAL", ""));
            this.TAG_NAME = properties.getProperty("TAG_NAME", "");
            this.TAG_PROPORTION = properties.getProperty("TAG_PROPORTION", "");

            this.FIELD_NUMBER = Integer.parseInt(properties.getProperty("FIELD_NUMBER", ""));
            this.FIELD_PROPORTION = properties.getProperty("FIELD_PROPORTION", "");

            this.TIME_INTERVAL = Long.parseLong(properties.getProperty("TIME_INTERVAL", ""));
            this.TIME_START = Long.parseLong(properties.getProperty("TIME_START", ""));
            this.TIME_END = Long.parseLong(properties.getProperty("TIME_END", ""));

            //队列中最多产生的batch数，每个batch有batchsize条record
            REASONABLE_CAPACITY = ((TIME_END - TIME_START) / TIME_INTERVAL) * getTAG_TOTAL() / getBATCH_SIZE();
            REASONABLE_CAPACITY = REASONABLE_CAPACITY > Integer.MAX_VALUE ?
                    3L * Integer.MAX_VALUE / 4 :
                    3L * REASONABLE_CAPACITY / 4;
        } catch (NumberFormatException e) {
            System.out.println("GetProperty failed! please check the properties!");
            e.printStackTrace();
        }
    }

    //静态内部类单例
    private static class ConfigHolder {
        private static final Config INSTANCE = new Config();
    }

    public static Config getInstance() {
        return ConfigHolder.INSTANCE;
    }


    public int getPRODUCER_NUMBER() {
        return PRODUCER_NUMBER;
    }

    public int getCONSUMER_NUMBER() {
        return CONSUMER_NUMBER;
    }

    public int getBATCH_SIZE() {
        return BATCH_SIZE;
    }

    public int getDEVICE_NUMBER() {
        return DEVICE_NUMBER;
    }

    public int getTAG_TOTAL() {
        return TAG_TOTAL;
    }

    public String getTAG_NAME() {
        return TAG_NAME;
    }

    public String getTAG_PROPORTION() {
        return TAG_PROPORTION;
    }

    public String getFIELD_PROPORTION() {
        return FIELD_PROPORTION;
    }

    public int getFIELD_NUMBER() {
        return FIELD_NUMBER;
    }

    public long getTIME_INTERVAL() {
        return TIME_INTERVAL;
    }

    public long getTIME_START() {
        return TIME_START;
    }

    public long getTIME_END() {
        return TIME_END;
    }

    //取0.75最大容量
    public long getREASONABLE_CAPACITY() {
        return REASONABLE_CAPACITY;
    }
}

package cn.edu.ruc.tsbenchmark.config;

import java.util.Properties;

public class DatabaseInfo {
    private static final Properties properties = Config.getInstance().getProperties();

    public static class InfluxInfo {
        public static String USERNAME = properties.getProperty("Influx_username");
        public static String PASSWORD = properties.getProperty("Influx_password");
        public static String DATABASE = properties.getProperty("Influx_database");
        public static String IP = properties.getProperty("Influx_ip");
        public static String PORT = properties.getProperty("Influx_port");
        public static String RETENTION_POLICY = properties.getProperty("Influx_retentionPolicy");
        public static String DURATION = properties.getProperty("Influx_duration");
        public static String REPLICATION = properties.getProperty("Influx_replication");
        public static String SHARD_DURATION = properties.getProperty("Influx_shard_duration");
    }




}

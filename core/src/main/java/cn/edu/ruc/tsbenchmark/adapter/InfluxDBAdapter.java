package cn.edu.ruc.tsbenchmark.adapter;

import cn.edu.ruc.tsbenchmark.config.DatabaseInfo.InfluxInfo;
import cn.edu.ruc.tsbenchmark.schema.Batch;
import cn.edu.ruc.tsbenchmark.schema.DataRecord;
import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

public class InfluxDBAdapter extends Adapter {
    private InfluxDB influxDB;
    private final String measurement = "ruc_test";

    public InfluxDBAdapter() {
        super();
    }

    @Override
    public void initConnect() {
        //connection
        String url = "http://" + InfluxInfo.IP + ":" + InfluxInfo.PORT;
        OkHttpClient.Builder builder = new OkHttpClient().newBuilder()
                .readTimeout(50000, TimeUnit.MILLISECONDS)
                .connectTimeout(50000, TimeUnit.MILLISECONDS)
                .writeTimeout(50000, TimeUnit.MILLISECONDS);

        influxDB = InfluxDBFactory.connect(url, InfluxInfo.USERNAME, InfluxInfo.PASSWORD, builder);

        //setDatabase
        if (!influxDB.databaseExists(InfluxInfo.DATABASE)) {
            influxDB.createDatabase(InfluxInfo.DATABASE);
        }
        influxDB.setDatabase(InfluxInfo.DATABASE);

        //createRetentionPolicy
        if ("true".equals(InfluxInfo.RETENTION_POLICY))
            influxDB.createRetentionPolicy("myRP",
                    InfluxInfo.DATABASE, InfluxInfo.DURATION, InfluxInfo.SHARD_DURATION,
                    Integer.parseInt(InfluxInfo.REPLICATION), false);

        //ping test
        try {
            if (influxDB.ping() == null)
                throw new Exception("InfluxDB connection failed!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //    @Override
//    public long insertData(Batch data) {
//        if (data.isEmpty())
//            return -1;
//        //pre
//        String[] fieldSchema = metaDataSchema.getFieldSchema();
//        LinkedList<DataRecord> recordList = data.getRecordList();
//        LinkedHashMap<String, String> tagMap = new LinkedHashMap<>();
//        LinkedHashMap<String, Object> fieldsMap = new LinkedHashMap<>();
//        BatchPoints batchPoints = BatchPoints.database(InfluxInfo.DATABASE).build();
//        //take
//        while (!recordList.isEmpty()) {
//            DataRecord record = recordList.removeFirst();
//            //time
//            long timeStamp = record.getTimeStamp();
//            //tag
//            String[] split = record.getTagString().split(",");
//            for (String expression : split) {
//                String[] entry = expression.split("=");
//                tagMap.put(entry[0], entry[1]);
//            }
//            //field
//            Object[] fieldsValue = record.getFieldsValue();
//            for (int i = 0; i < fieldsValue.length; i++)
//                fieldsMap.put(fieldSchema[i], fieldsValue[i]);
//
//            influxDB.write(Point.measurement("ruc_test")
//                    //.time(timeStamp, TimeUnit.MILLISECONDS)
//                    .tag(new LinkedHashMap<>(tagMap))
//                    .fields(new LinkedHashMap<>(fieldsMap)).build());
//
//            //write batchPoints
////            batchPoints.point(Point.measurement("ruc_test")
////                    //.time(timeStamp, TimeUnit.MILLISECONDS)
////                    .tag(new LinkedHashMap<>(tagMap))
////                    .fields(new LinkedHashMap<>(fieldsMap)).build());
//
//            tagMap.clear();
//            fieldsMap.clear();
//        }
//
//        //calculate
//        long start = System.currentTimeMillis();
//        //influxDB.write(batchPoints);
//        long end = System.currentTimeMillis();
//        return end - start;
//    }
    @Override
    public long insertData(Batch data) {
        if (data.isEmpty())
            return -1;
        //pre
        StringBuilder insertQL = new StringBuilder();
        String[] fieldSchema = metaDataSchema.getFieldSchema();
        LinkedList<DataRecord> recordList = data.getRecordList();
        LinkedList<String> list = new LinkedList<>();

        //take
        while (!recordList.isEmpty()) {
            insertQL.append(measurement).append(',');
            DataRecord record = recordList.removeFirst();
            //tag
            insertQL.append(record.getTagString()).append(' ');

            //field
            Object[] fieldsValue = record.getFieldsValue();
            for (int i = 0; i < fieldsValue.length; i++) {
                insertQL.append(fieldSchema[i]).append('=').append(fieldsValue[i]).append(',');
            }
            //time 先去掉最后一个逗号
            insertQL.deleteCharAt(insertQL.length() - 1);
            insertQL.append(' ').append(record.getTimeStamp());
            list.add(insertQL.toString());
            insertQL.setLength(0);
        }
        //calculate
        long start = System.currentTimeMillis();
        influxDB.write(list);
        long end = System.currentTimeMillis();
        return end - start;
    }

    @Override
    public long getCount() {
        String name = metaDataSchema.getFieldSchema()[0];
        QueryResult query = influxDB.query(
                new Query("select count(" + name + ") from " + measurement, InfluxInfo.DATABASE));
        String res = String.valueOf(query.getResults().get(0).getSeries().get(0).getValues().get(0).get(1));
        //返回的是个Double,以小数点切割取左边
        return Long.parseLong(res.split("\\.")[0]);
    }

    @Override
    public void closeConnection() {
        influxDB.close();
    }
}


import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

public class InfluxdbTest {

//    // 用户名
//    private String username;
//    // 密码
//    private String password;
//    // 连接地址
//    private String openurl;
//    // 数据库
//    private String database;
//    // 保留策略
//    private String retentionPolicy;

    private InfluxDB influxDB;

    private final String database = "ruc";
    private final String measurement = "ruc_test";


    @Before
    public void initConnection() {
        String ip = "127.0.0.1";
        influxDB = InfluxDBFactory.connect("http://" + ip + ":8086", "root", "root");
        String database = "ruc";
        if (!influxDB.databaseExists(database)) {
            influxDB.createDatabase(database);
        }
        //influxDB.setRetentionPolicy();
        influxDB.setDatabase(database);
    }

    @Test
    public void pingTest() {
        Pong ping = influxDB.ping();
        if (ping == null)
            System.out.println("ping failed!");
        else
            System.out.println("ping success!");
    }

    @Test
    public void insertOnePoint() {

        HashMap<String, String> tagMap = new HashMap<>();
        HashMap<String, Object> fieldsMap = new HashMap<>();
        tagMap.put("user", "jim");
        tagMap.put("sever", "aliyun");
        fieldsMap.put("address", "ruc6061");
        fieldsMap.put("weight", 50.92);
        fieldsMap.put("age", new Integer(30));
        fieldsMap.put("married", false);

        Point point = Point.measurement("ruc_test").tag(tagMap).fields(fieldsMap).build();
        influxDB.write(point);
    }

    @Test
    public void insertOneRecord1() {
        StringBuilder sb = new StringBuilder();
        sb.append(measurement).append(",user=").append('"').append("jim").append('"').append(' ').append("age=99");
        //String s = measurement + ",user=\"jim\",sever=\"aliyun\" address=\"ruc6061\",weight=50.92,age=30i,married=false";

        influxDB.write(sb.toString());
    }

    @Test
    public void insertOneBatch() {
        String database = "ruc";
        if (!influxDB.databaseExists(database)) {
            influxDB.createDatabase(database);
        }
        influxDB.setDatabase(database);
    }

    @Test
    public void insertOneRecord2() {

        String s = "ruc_test,area=0,site=0,user=0,server=0,cluster=0,hostname=0 " +
                "I=5 " +
                "1630051380000";
        influxDB.write(s);
    }

    @Test
    public void getCount() {
        QueryResult query = influxDB.query(new Query("select count(I0) from " + measurement, database));
        String res = String.valueOf(query.getResults().get(0).getSeries().get(0).getValues().get(0).get(1));
        System.out.println(Long.parseLong(res.split("\\.")[0]));
    }

    @Test
    public void dropDatabase(){
        influxDB.deleteDatabase(database);
    }

    @After
    public void closeConnection() {
        influxDB.close();
    }

//    public InfluxdbTest(String username, String password, String openurl, String database,
//                        String retentionPolicy) {
//        this.username = username;
//        this.password = password;
//        this.openurl = openurl;
//        this.database = database;
//        this.retentionPolicy = retentionPolicy == null || retentionPolicy.equals("") ? "autogen" : retentionPolicy;
//        influxDbBuild();
//    }

//    /**
//     * 创建数据库
//     *
//     * @param dbName
//     */
//    @SuppressWarnings("deprecation")
//    public void createDB(String dbName) {
//        influxDB.createDatabase(dbName);
//    }
//
//    /**
//     * 删除数据库
//     *
//     * @param dbName
//     */
//    @SuppressWarnings("deprecation")
//    public void deleteDB(String dbName) {
//        influxDB.deleteDatabase(dbName);
//    }
//
//    /**
//     * 测试连接是否正常
//     *
//     * @return true 正常
//     */
//    public boolean ping() {
//        boolean isConnected = false;
//        Pong pong;
//        try {
//            pong = influxDB.ping();
//            if (pong != null) {
//                isConnected = true;
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return isConnected;
//    }
//
//    /**
//     * 连接时序数据库 ，若不存在则创建
//     *
//     * @return
//     */
//    public InfluxDB influxDbBuild() {
//        if (influxDB == null) {
//            influxDB = InfluxDBFactory.connect(openurl, username, password);
//        }
//        try {
//            // if (!influxDB.databaseExists(database)) {
//            // influxDB.createDatabase(database);
//            // }
//        } catch (Exception e) {
//            // 该数据库可能设置动态代理，不支持创建数据库
//            // e.printStackTrace();
//        } finally {
//            influxDB.setRetentionPolicy(retentionPolicy);
//        }
//        influxDB.setLogLevel(InfluxDB.LogLevel.NONE);
//        return influxDB;
//    }
//
//    /**
//     * 创建自定义保留策略
//     *
//     * @param policyName  策略名
//     * @param duration    保存天数
//     * @param replication 保存副本数量
//     * @param isDefault   是否设为默认保留策略
//     */
//    public void createRetentionPolicy(String policyName, String duration, int replication, Boolean isDefault) {
//        String sql = String.format("CREATE RETENTION POLICY %s ON  % s DURATION %s REPLICATION %s ", policyName,
//                database, duration, replication);
//        if (isDefault) {
//            sql = sql + " DEFAULT";
//        }
//        this.query(sql);
//    }
//
//    /**
//     * 创建默认的保留策略
//     * 策略名
//     *
//     * @param ：default，保存天数：30天，保存副本数量：1 设为默认保留策略
//     */
//    public void createDefaultRetentionPolicy() {
//        String command = String.format("CREATE RETENTION POLICY  %s ON  %s DURATION %s REPLICATION %s DEFAULT",
//                "default", database, "30d", 1);
//        this.query(command);
//    }
//
//    /**
//     * 查询
//     *
//     * @param command 查询语句
//     * @return
//     */
//    public QueryResult query(String command) {
//        return influxDB.query(new Query(command, database));
//    }
//
//    /**
//     * 插入
//     *
//     * @param measurement 表
//     * @param tags        标签
//     * @param fields      字段
//     */
//    public void insert(String measurement, Map<String, String> tags, Map<String, Object> fields, long time,
//                       TimeUnit timeUnit) {
//        Builder builder = Point.measurement(measurement);
//        builder.tag(tags);
//        builder.fields(fields);
//        if (0 != time) {
//            builder.time(time, timeUnit);
//        }
//        influxDB.write(database, retentionPolicy, builder.build());
//    }
//
//    /**
//     * 批量写入测点
//     *
//     * @param batchPoints
//     */
//    public void batchInsert(BatchPoints batchPoints) {
//        influxDB.write(batchPoints);
//        // influxDB.enableGzip();
//        // influxDB.enableBatch(2000,100,TimeUnit.MILLISECONDS);
//        // influxDB.disableGzip();
//        // influxDB.disableBatch();
//    }
//
//    /**
//     * 批量写入数据
//     *
//     * @param database        数据库
//     * @param retentionPolicy 保存策略
//     * @param consistency     一致性
//     * @param records         要保存的数据（调用BatchPoints.lineProtocol()可得到一条record）
//     */
//    public void batchInsert(final String database, final String retentionPolicy, final ConsistencyLevel consistency,
//                            final List<String> records) {
//        influxDB.write(database, retentionPolicy, consistency, records);
//    }
//
//    /**
//     * 删除
//     *
//     * @param command 删除语句
//     * @return 返回错误信息
//     */
//    public String deleteMeasurementData(String command) {
//        QueryResult result = influxDB.query(new Query(command, database));
//        return result.getError();
//    }
//
//    /**
//     * 关闭数据库
//     */
//    public void close() {
//        influxDB.close();
//    }
//
//    /**
//     * 构建Point
//     *
//     * @param measurement
//     * @param time
//     * @param fields
//     * @return
//     */
//    public Point pointBuilder(String measurement, long time, Map<String, String> tags, Map<String, Object> fields) {
//        Point point = Point.measurement(measurement).time(time, TimeUnit.MILLISECONDS).tag(tags).fields(fields).build();
//        return point;
//    }

}


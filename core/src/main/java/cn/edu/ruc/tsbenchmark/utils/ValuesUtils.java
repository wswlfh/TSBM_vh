package cn.edu.ruc.tsbenchmark.utils;

import cn.edu.ruc.tsbenchmark.config.Config;
import cn.edu.ruc.tsbenchmark.schema.MetaDataSchema;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class ValuesUtils extends ConcurrentHashMap<Integer, Object[]> {
    private static final Config config = Config.getInstance();
    private static final int[] fieldTypes = MetaDataSchema.getInstance().getFieldTypes();
    private static final int capacity = Math.min((int) (config.getTHEORETICAL_SIZE() / 60), 10000000);

    private ValuesUtils() {
        super(capacity);
        System.out.println(DateUtils.getDate() + " Start Continuously generate " + capacity + " pieces of random values and insert them into the valuesMap.............\n");
        long start = System.currentTimeMillis();
        Thread t = new Thread(() -> {
            //AtomicReference<String> s = new AtomicReference<>();
            for (int i = 0; i < capacity; i++) {
                //s.set(getRandomV());
                this.put(i, getArrayRandomV());
            }
            System.out.println(DateUtils.getDate() + " A total of " + capacity + " random values are inserted,it takes a total of " +
                    (System.currentTimeMillis() - start) / 1000 + "s");
        });
        t.setPriority(7);
        t.start();
    }

    private static class ValuesMapsHolder {
        private static final ValuesUtils INSTANCE = new ValuesUtils();
    }

    public static ValuesUtils getInstance() {
        return ValuesUtils.ValuesMapsHolder.INSTANCE;
    }


    public Object[] get(Integer key) {
        int newKey = Math.abs((key.hashCode() - key + getIntByRandom()) % capacity);
        if (!containsKey(newKey))
            this.put(newKey, getArrayRandomV());
        return super.get(newKey);
    }

    private static String getStringRandomV() {
        StringBuilder sb = new StringBuilder();
        int i = 0, d = 0, l = 0, b = 0, s = 0, date = 0;
        for (int k = 0; k < config.getFIELD_NUMBER(); k++) {
            if (fieldTypes[k] == 0)
                sb.append("i").append(i++).append('=').append(getIntByRandom()).append(',');
            else if (fieldTypes[k] == 1)
                sb.append("d").append(d++).append('=').append(getDoubleByRandom(d)).append(',');
            else if (fieldTypes[k] == 2)
                sb.append("l").append(l++).append('=').append(getLongByRandom()).append(',');
            else if (fieldTypes[k] == 3)
                sb.append("b").append(b++).append('=').append(getBooleanByRandom()).append(',');
            else if (fieldTypes[k] == 4)
                sb.append("s").append(s++).append('=').append(getStringByRandom()).append(',');
            else if (fieldTypes[k] == 5)
                sb.append("date").append(date++).append('=').append(getDateByRandom()).append(',');
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    private static Object[] getArrayRandomV() {
        Object[] values = new Object[config.getFIELD_NUMBER()];
        for (int k = 0; k < values.length; k++) {
            if (fieldTypes[k] == 0)
                values[k] = getIntByRandom();
            else if (fieldTypes[k] == 1)
                values[k] = getDoubleByRandom(k);
            else if (fieldTypes[k] == 2)
                values[k] = getLongByRandom();
            else if (fieldTypes[k] == 3)
                values[k] = getBooleanByRandom();
            else if (fieldTypes[k] == 4)
                values[k] = getStringByRandom();
            else if (fieldTypes[k] == 5)
                values[k] = getDateByRandom();
        }
        return values;
    }

    private static double getDoubleByRandom(int index) {
        int bound = index % 2 == 0 ? 100 : 10000;
        double d = ThreadLocalRandom.current().nextDouble(0, bound);
        d = (double) Math.round(d * 1000) / 1000;
        return d;
    }

    private static int getIntByRandom() {
        return ThreadLocalRandom.current().nextInt(0, 10000);
    }

    private static boolean getBooleanByRandom() {
        return ThreadLocalRandom.current().nextBoolean();
    }

    private static long getLongByRandom() {
        return ThreadLocalRandom.current().nextLong(0, 10000000);
    }

    //默认生成10位字母的字符串
    private static String getStringByRandom() {
        int length = 5;
        StringBuilder buffer = new StringBuilder("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");
        StringBuilder sb = new StringBuilder();

        int range = buffer.length();
        for (int i = 0; i < length; i++) {
            sb.append(buffer.charAt(ThreadLocalRandom.current().nextInt(range)));
        }
        return sb.toString();
    }

    //默认生成1999-2022的随机一天日期
    private static Date getDateByRandom() {
        int startYear = 1999;                                    //指定随机日期开始年份
        int endYear = 2022;                                    //指定随机日期开始年份(含)
        long start = Timestamp.valueOf(startYear + 1 + "-1-1 0:0:0").getTime();
        long end = Timestamp.valueOf(endYear + "-1-1 0:0:0").getTime();
        long ms = (long) ((end - start) * ThreadLocalRandom.current().nextDouble() + start);    //获得了符合条件的13位毫秒数
        return new Date(ms);
    }


    public static void main(String[] args) {
//        for (int i = 0; i < 10; i++) {
//            System.out.println(getIntByRandom());
//            System.out.println(getDoubleByRandom());
//            System.out.println(getLongByRandom());
//            System.out.println(getBooleanByRandom());
//            System.out.println(getStringByRandom());
//            System.out.println(getDateByRandom());
//            System.out.println();
//        }
        getInstance();
    }
}

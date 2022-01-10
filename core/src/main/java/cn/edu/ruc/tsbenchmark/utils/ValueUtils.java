package cn.edu.ruc.tsbenchmark.utils;

import cn.edu.ruc.tsbenchmark.config.Config;
import cn.edu.ruc.tsbenchmark.schema.MetaDataSchema;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.concurrent.ThreadLocalRandom;

public class ValueUtils {
    private static final Config config = Config.getInstance();
    private static final MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();

    public static int[] ints = new int[1000000];
    public static double[] doubles = new double[1000000];
    public static final long[] longs = new long[1000000];
    public static final boolean[] bools = new boolean[1000000];
    public static final String[] strings = new String[1000000];
    public static final String[] dates = new String[1000000];

//    ValueUtils() {
//        genRandom();
//    }
//
//    private static class ValueUtilsHolder {
//        private static final ValueUtils INSTANCE = new ValueUtils();
//    }
//
//    public static ValueUtils getInstance() {
//        return ValueUtils.ValueUtilsHolder.INSTANCE;
//    }

    public static void genRandom() {
        for (int i = 0; i < config.getLOOP() + 10; i++) {
            ints[i] = getIntByRandom();
            doubles[i] = getDoubleByRandom();
            longs[i] = getLongByRandom();
            bools[i] = getBooleanByRandom();
            strings[i] = getStringByRandom();
            dates[i] = String.valueOf(getDateByRandom());
        }
    }

    public static Object[] getRandomV(int index) {
        Object[] values = new Object[config.getFIELD_NUMBER()];
        String[] fieldTypes = metaDataSchema.getFieldTypes();
        int length = ints.length;
        int hashcode = 0;
        for (int i = 0; i < values.length; i++) {
            if (i != 0) hashcode = values[i - 1].hashCode();
            int seed = Math.abs((hashcode - index) % length);
            if ("I".equals(fieldTypes[i]))
                values[i] = ints[seed];
            else if ("D".equals(fieldTypes[i]))
                values[i] = doubles[seed];
            else if ("L".equals(fieldTypes[i]))
                values[i] = longs[seed];
            else if ("B".equals(fieldTypes[i]))
                values[i] = bools[seed];
            else if ("S".equals(fieldTypes[i]))
                values[i] = strings[seed];
            else if ("Date".equals(fieldTypes[i]))
                values[i] = dates[seed];
        }
        return values;
    }

    private static double getDoubleByRandom() {
        double d = ThreadLocalRandom.current().nextDouble(1, 20000);
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

    }
}

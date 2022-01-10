package cn.edu.ruc.tsbenchmark.utils;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class ValueUtils {

    public static LinkedHashMap<String, String> getFieldValues(String[] fieldTypes) {
        LinkedHashMap<String, String> fieldValues = new LinkedHashMap<>();
        int i = 0, d = 0, l = 0, b = 0, s = 0, date = 0;
        for (int k = 0; k < fieldTypes.length; k++) {
            if (fieldTypes[k].equals("Integer"))
                fieldValues.put("Int" + i++, String.valueOf(getIntByRandom()));
            else if (fieldTypes[k].equals("Double"))
                fieldValues.put("D" + d++, String.valueOf(getDoubleByRandom()));
            else if (fieldTypes[k].equals("Long"))
                fieldValues.put("L" + l++, String.valueOf(getLongByRandom()));
            else if (fieldTypes[k].equals("Boolean"))
                fieldValues.put("B" + b++, String.valueOf(getBooleanByRandom()));
            else if (fieldTypes[k].equals("String"))
                fieldValues.put("S" + s++, getStringByRandom(10));
            else if (fieldTypes[k].equals("Date"))
                fieldValues.put("Date" + date++, String.valueOf(getDateByRandom()));
            else
                throw new IllegalArgumentException("Unsupported field type");
        }
        return fieldValues;
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
    private static String getStringByRandom(int length) {
        StringBuffer buffer = new StringBuffer("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");
        StringBuffer sb = new StringBuffer();

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
        for (int i = 0; i < 10; i++) {
            System.out.println(getIntByRandom());
            System.out.println(getDoubleByRandom());
            System.out.println(getLongByRandom());
            System.out.println(getBooleanByRandom());
            System.out.println(getStringByRandom(10));
            System.out.println(getDateByRandom());
            System.out.println();
        }
    }
}

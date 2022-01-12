package cn.edu.ruc.tsbenchmark.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {
    public static String getDate() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dateFormat.format(new Date());
    }
}

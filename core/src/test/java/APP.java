import org.junit.Test;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

public class APP {
    @Test
    public void test1() {
        String s = "s1=%s,%s";
        String res = String.format(s, new Object[]{1, 2});
        System.out.println(res);
    }

    @Test
    public void test2() {
        long start = System.nanoTime();
        Object[] ints = new Object[100000000];
        Random random = new Random();
        for (int i = 0; i < 100000000; i++) {
            ints[i] = random.nextInt(1000000);
        }
        long end = System.nanoTime();
        System.out.println((end - start) / 1000 / 1000);
    }

    @Test
    public void test3() {
        long start = System.nanoTime();
        Object[] ints = new Object[100000000];
        ints[0] = 0;
        for (int i = 0; i < 100000000; i++) {
            if (i != 0)
                ints[i] = Math.abs(ints[i - 1].hashCode());
        }
        long end = System.nanoTime();
        System.out.println((end - start) / 1000 / 1000);
    }

    @Test
    public void test4() {
        long start = System.nanoTime();
        ConcurrentLinkedQueue<Object[]> queue = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < 1000000000; i++) {
            this.hashCode();
        }
        long end = System.nanoTime();
        System.out.println((end - start) / 1000 / 1000);
    }

    @Test
    public void test5() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(dateFormat.format(new Date()));
    }

    @Test
    public void test6() {
        Object[] objects = {1231, 762, 343, 2344, 5.213, 5126.123, 3217.111, 3128.321, 1239.311, 0.31, 0.612, 0.1212, 0.912, 123.12, 15, 16, 17, 18, 12312321};
        int length = 10000000;
        StringBuilder sb = new StringBuilder();
        long start1 = System.currentTimeMillis();
        for (int i = 0; i < length; i++) {
            for (Object object : objects) {
                sb.append("I=").append(object).append(',');
            }
            sb.toString();
            sb.setLength(0);
        }
        long end1 = System.currentTimeMillis();
        System.out.println(end1 - start1);

        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        long start2 = System.currentTimeMillis();
        for (int i = 0; i < length; i++) {
            for (Object object : objects) {
                map.put("I" + i, object);
            }
            map.clear();
        }
        long end2 = System.currentTimeMillis();
        System.out.println(end2 - start2);
    }

    //根据每条时间线所需数据数量计算出end时间戳
    @Test
    public void test7() {
        int count = 30000;
        long start = 1629877620000L;
        long interval = 60000;

        long newEnd = count * interval + start;
        System.out.println(newEnd);
        System.out.println(((newEnd - 1629877620000L) / 60000) == count);
    }


    @Test
    public void test8() {
        int n = 1000;

        BigInteger sum = BigInteger.valueOf(1L);
        for (int i = 1; i <= n; ++i) {
            sum = sum.multiply(BigInteger.valueOf(i));
        }

        System.out.println(sum);
    }

    @Test
    public void test9() {
        long start = System.currentTimeMillis();

        int n = 100000;
        int[] nums = new int[n];
        for (int i = 0; i < nums.length; i++) {
            nums[i] = new Random().nextInt(10000);
        }

        //冒泡
        for (int i = 0; i < n; ++i) {
            for (int j = 0; j < n - 1; ++j) {
                if (nums[j] > nums[j + 1]) {
                    int temp = nums[j + 1];
                    nums[j + 1] = nums[j];
                    nums[j] = temp;
                }
            }
        }

        //快排
        //Arrays.sort(nums);


        long end = System.currentTimeMillis();

        System.out.println(end - start + "ms");
    }

}

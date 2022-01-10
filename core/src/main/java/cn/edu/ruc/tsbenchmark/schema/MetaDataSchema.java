package cn.edu.ruc.tsbenchmark.schema;

import cn.edu.ruc.tsbenchmark.config.Config;

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MetaDataSchema {
    private static final Config config = Config.getInstance();
    private String[] tagNames;
    private int[] tagProportion;

    private final ConcurrentHashMap<Integer, Deque<String>> tagsMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Deque<String>> recordListMap = new ConcurrentHashMap<>();
    private String[] fieldTypes;
    private int[] fieldProportion;

    public AtomicLong size = new AtomicLong();

    MetaDataSchema() {
        initTags();
        initFileds();
        creatSchema();
    }


    private void initTags() {
        int total = config.getTAG_TOTAL();
        String[] names = config.getTAG_NAME().split(":");
        String[] proportions = config.getTAG_PROPORTION().split(":");
        try {
            tagProportion = new int[proportions.length];
            int sum = 1;
            for (int i = 0; i < proportions.length; i++) {
                int p = Integer.parseInt(proportions[i]);
                sum *= p;
                tagProportion[i] = p;
            }

            if (names.length != proportions.length || sum != total)
                throw new IllegalArgumentException();
        } catch (Exception e) {
            System.out.println("Parameters about tag are not configured correctly");
            e.printStackTrace();
        }
        tagNames = names;

    }

    private void initFileds() {
        int number = config.getFIELD_NUMBER();
        String[] proportions = config.getFIELD_PROPORTION().split(":");
        fieldProportion = new int[proportions.length];
        try {
            int sum = 0;
            for (int i = 0; i < proportions.length; i++) {
                int p = Integer.parseInt(proportions[i]);
                sum += p;
                fieldProportion[i] = p;
            }

            if (sum != number)
                throw new IllegalArgumentException();
        } catch (Exception e) {
            System.out.println("Parameters about filed are not configured correctly");
            e.printStackTrace();
        }
        fieldTypes = new String[number];
        int i = 0;
        for (int j = 0; j < proportions.length; j++) {
            String field;
            if (j == 0) field = "Integer";
            else if (j == 1) field = "Double";
            else if (j == 2) field = "Long";
            else if (j == 3) field = "Boolean";
            else if (j == 4) field = "String";
            else if (j == 5) field = "Date";
            else throw new IllegalArgumentException("Unsupported field type");


            for (int k = 0; k < Integer.parseInt(proportions[j]); k++) {
                fieldTypes[i++] = field;
            }
        }


    }

    private static class SchemaHolder {
        private static final MetaDataSchema INSTANCE = new MetaDataSchema();
    }

    public static MetaDataSchema getInstance() {
        return SchemaHolder.INSTANCE;
    }


    //采用 遍历0-时间线总数 的数index 模上各个tag的比例，得出对应值
    private void creatSchema() {
        int total = config.getTAG_TOTAL();
        int per = total / config.getPRODUCER_NUMBER();

        for (int i = 0; i < config.getPRODUCER_NUMBER() - 1; i++) {
            tagsMap.put(i, new LinkedList<>());
            for (int j = i * per; j < i * per + per; j++)
                tagsMap.get(i).add(getTagsString(j));
        }
        //last Client
        tagsMap.put(config.getPRODUCER_NUMBER() - 1, new LinkedList<>());
        for (int i = (config.getPRODUCER_NUMBER() - 1) * per; i < total; i++) {
            tagsMap.get(config.getPRODUCER_NUMBER() - 1).add(getTagsString(i));
        }
    }

    private String getTagsString(int index) {
        String[] tagNames = getTagNames();
        int[] tagProportion = getTagProportion();
        int[] values = new int[tagProportion.length];
        StringBuilder sb = new StringBuilder();
        for (int i = values.length - 1; i >= 0; i--) {
            values[i] = index % tagProportion[i];
            index /= tagProportion[i];
            sb.insert(0, tagNames[i] + "=" + values[i]);
            if (i != 0) sb.insert(0, ',');
        }
        return sb.toString();
    }


    public String[] getTagNames() {
        return tagNames;
    }

    public int[] getTagProportion() {
        return tagProportion;
    }

    public String[] getFieldTypes() {
        return fieldTypes;
    }

    public int[] getFieldProportion() {
        return fieldProportion;
    }

    public ConcurrentHashMap<Integer, Deque<String>> getTagsMap() {
        return tagsMap;
    }

    public ConcurrentHashMap<Integer, Deque<String>> getRecordListMap() {
        return recordListMap;
    }

}

package cn.edu.ruc.tsbenchmark.schema;

import cn.edu.ruc.tsbenchmark.config.Config;

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

public class MetaDataSchema {
    private static final Config config = Config.getInstance();
    private String[] tagNames;
    private int[] tagProportion;
    private final ConcurrentHashMap<Integer, Deque<String>> tagsMap = new ConcurrentHashMap<>();
    private int[] fieldTypes; //0:Integer  1:Double  2:Long  3:Boolean  4:String  5:Date
    private int[] fieldProportion;
    private String fieldSchema;


    //public AtomicLong size = new AtomicLong();

    private static class SchemaHolder {
        private static final MetaDataSchema INSTANCE = new MetaDataSchema();
    }

    public static MetaDataSchema getInstance() {
        return SchemaHolder.INSTANCE;
    }

    MetaDataSchema() {
        initTags();
        initFields();
        creatTagSchema();
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

    private void initFields() {
        int number = config.getFIELD_NUMBER();
        String[] proportions = config.getFIELD_PROPORTION().split(":");
        fieldProportion = new int[proportions.length];
        fieldTypes = new int[config.getFIELD_NUMBER()];
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
        int i = 0;
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < proportions.length; j++) {
            String field;
            if (j == 0) field = "I";
            else if (j == 1) field = "D";
            else if (j == 2) field = "L";
            else if (j == 3) field = "B";
            else if (j == 4) field = "S";
            else if (j == 5) field = "Date";
            else throw new IllegalArgumentException("Unsupported field type");

            for (int k = 0; k < Integer.parseInt(proportions[j]); k++) {
                sb.append(field).append(k).append("=").append("%s,");
                fieldTypes[i++] = j;
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        fieldSchema = sb.toString();
    }

    //采用 遍历0-时间线总数 的数index 模上各个tag的比例，得出对应值
    private void creatTagSchema() {
        int total = config.getTAG_TOTAL();
        int per = total / config.getPRODUCER_NUMBER();

        for (int i = 0; i < config.getPRODUCER_NUMBER() - 1; i++) {
            tagsMap.put(i, new LinkedList<>());
            for (int j = i * per; j < i * per + per; j++)
                tagsMap.get(i).add(getTagValue(j));
        }
        //last Client
        tagsMap.put(config.getPRODUCER_NUMBER() - 1, new LinkedList<>());
        for (int i = (config.getPRODUCER_NUMBER() - 1) * per; i < total; i++) {
            tagsMap.get(config.getPRODUCER_NUMBER() - 1).add(getTagValue(i));
        }
    }

    private String getTagValue(int index) {
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

    public String getFieldSchema() {
        return fieldSchema;
    }

    public int[] getFieldProportion() {
        return fieldProportion;
    }

    public ConcurrentHashMap<Integer, Deque<String>> getTagsMap() {
        return tagsMap;
    }

    public int[] getFieldTypes() {
        return fieldTypes;
    }
}

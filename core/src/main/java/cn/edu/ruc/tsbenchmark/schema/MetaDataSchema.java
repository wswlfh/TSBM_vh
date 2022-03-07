package cn.edu.ruc.tsbenchmark.schema;

import cn.edu.ruc.tsbenchmark.config.Config;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

public class MetaDataSchema {
    private static final Config config = Config.getInstance();
    private String[] tagNames;
    private int[] tagProportion;
    private final ArrayList<String> tagsList = new ArrayList<>();
    private final ArrayList<Long> timestampList = new ArrayList<>();
    private boolean[][] timeSeriesTable;
    private final ConcurrentHashMap<Integer, Deque<Batch>> productMean = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Boolean> batchStatus = new ConcurrentHashMap<>(); //记录每个batch的写入情况

    private int[] fieldTypes; //0:Integer  1:Double  2:Long  3:Boolean  4:String  5:Date
    private int[] fieldProportion;
    private String[] fieldSchema;
    private int batchTotal;


    private static class SchemaHolder {
        private static final MetaDataSchema INSTANCE = new MetaDataSchema();
    }

    public static MetaDataSchema getInstance() {
        return SchemaHolder.INSTANCE;
    }

    MetaDataSchema() {
        initTags();
        initFields();
        initTimeSeriesTable();
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
            System.exit(-1);
        }
        tagNames = names;

        //创建tag的schema
        for (int i = 0; i < config.getTAG_TOTAL(); i++) {
            tagsList.add(getTagValue(i));
        }

        //创建时间戳序列
        for (long start = config.getTIME_START() - config.getTIME_INTERVAL(); start < config.getTIME_END(); ) {
            start = Math.min(config.getTIME_END(), start + config.getTIME_INTERVAL());
            timestampList.add(start);
        }

    }

    private void initFields() {
        int number = config.getFIELD_NUMBER();
        String[] proportions = config.getFIELD_PROPORTION().split(":");
        fieldProportion = new int[proportions.length];
        fieldTypes = new int[config.getFIELD_NUMBER()];
        fieldSchema = new String[config.getFIELD_NUMBER()];
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
                fieldSchema[i] = field + "" + k;
                fieldTypes[i] = j;
                i++;
            }
        }
    }

    //初始化时序表：以Map形式存储时间戳-节点布尔二维矩阵
    private void initTimeSeriesTable() {
        //由于java语言特性，这里timeSeriesTable中为false的代表该时间戳-节点有效
        //type0: 全为false
        timeSeriesTable = new boolean[timestampList.size()][tagsList.size()];
        allocateToProductClient();
        timeSeriesTable = null;
    }


    //按照生产线程id，分配指定的每个生产线程的生产的batch批次(ID)
    private void allocateToProductClient() {
        long startIndex = 0, endIndex = 0;
        int batchId = 0, pId = 0;
        for (int i = 0; i < timestampList.size(); i++) {
            for (int j = 0; j < tagsList.size(); j++) {
                if (timeSeriesTable[i][j]) continue;
                //做好batch的schema信息，到生产时调用batch.addRecordList()添加tag和field值
                if ((endIndex + 1) % config.getBATCH_SIZE() == 0) {
                    pId = batchId % config.getPRODUCER_NUMBER();
                    Deque<Batch> batchDeque = productMean.getOrDefault(pId, new LinkedList<>());
                    batchDeque.add(new Batch(pId, batchId, startIndex, endIndex));
                    productMean.put(pId, batchDeque);
                    startIndex = endIndex + 1;
                    batchStatus.put(batchId++, false); //false表示：此batch未写入到db
                }
                endIndex++;
            }
        }
        //剩余的组成一个batch添加
        if (startIndex != endIndex)
            productMean.get(pId).add(new Batch(pId, batchId, startIndex, endIndex));

        batchStatus.put(batchId, false);
        batchTotal = startIndex == endIndex ? batchId - 1 : batchId;
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

    public String[] getFieldSchema() {
        return fieldSchema;
    }

    public int[] getFieldProportion() {
        return fieldProportion;
    }

    public ArrayList<String> getTagsList() {
        return tagsList;
    }

    public ArrayList<Long> getTimestampList() {
        return timestampList;
    }

    public int[] getFieldTypes() {
        return fieldTypes;
    }


    public ConcurrentHashMap<Integer, Deque<Batch>> getProductMean() {
        return productMean;
    }

    public int getBatchTotal() {
        return batchTotal;
    }

    public ConcurrentHashMap<Integer, Boolean> getBatchStatus() {
        return batchStatus;
    }
}

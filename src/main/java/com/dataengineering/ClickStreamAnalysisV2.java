package com.dataengineering;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class ClickStreamAnalysisV2 {

    private static final String FORMAT_OF_FILE_PATH = "%s/%s/%s";
    private static final String FORMAT_OF_EXECUTION_NAME = "SQL Batches: %s";

    private static final String NAME_OF_DATASET = "ClickStream";
    private static final String FIELD_DELIM = "|";
    private static final int NUM_FILES = 1;

    public static void main(String[] args) throws Exception {

        if (args.length != 1)
            throw new RuntimeException("Input file name wasn't passed! Please, pass input file name as parameter via command line...");

        if (args[0].equals(""))
            throw new RuntimeException("Input file name is not empty passed! Please, pass input file name as parameter via command line...");

        String dataDir = System.getenv("DATA_DIR");
        if (dataDir == null || dataDir.equals(""))
            throw new RuntimeException("DATA_DIR is empty! Please, define DATA_DIR environment variable...");

        String inputFilename = args[0];

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        String inputFileName = String.format(FORMAT_OF_FILE_PATH, dataDir, "input", inputFilename);
        DataSet<ClickStreamData> clickStreamDataSet = createClickStreamDataSet(env, inputFileName);
        tEnv.registerDataSet(NAME_OF_DATASET, clickStreamDataSet);

        DataSet<?> q1DataSet = executeQ1(tEnv);
        saveAsCsv(q1DataSet, dataDir, "q1");

        DataSet<?> q2DataSet = executeQ2(tEnv);
        saveAsCsv(q2DataSet, dataDir, "q2");

        DataSet<?> q3DataSet = executeQ3(tEnv);
        saveAsCsv(q3DataSet, dataDir, "q3");

        DataSet<?> q4DataSet = executeQ4(tEnv);
        saveAsCsv(q4DataSet, dataDir, "q4");

        DataSet<?> q5DataSet = executeQ5(tEnv);
        saveAsCsv(q5DataSet, dataDir, "q5");

        JobExecutionResult res = env.execute(String.format(FORMAT_OF_EXECUTION_NAME, ClickStreamAnalysisV2.class.getCanonicalName()));
    }

    // Q1 - Unique Product View counts by ProductId
    public static DataSet<Tuple2<Long, Long>> executeQ1(BatchTableEnvironment tEnv) {
        Table table = tEnv.sqlQuery("SELECT productId, count(*) AS countProductView FROM ClickStream WHERE eventName = 'view' GROUP BY productId");
        TupleTypeInfo<Tuple2<Long, Long>> tupleTypeInfo = new TupleTypeInfo<>(Types.LONG, Types.LONG);
        return tEnv.toDataSet(table, tupleTypeInfo);
    }

    // Q2 - Unique Event counts
    public static DataSet<Tuple2<String, Long>> executeQ2(BatchTableEnvironment tEnv) {
        Table table = tEnv.sqlQuery("SELECT eventName, count(*) AS countEvent FROM ClickStream GROUP BY eventName");
        TupleTypeInfo<Tuple2<String, Long>> tupleTypeInfo = new TupleTypeInfo<>(Types.STRING, Types.LONG);
        return tEnv.toDataSet(table, tupleTypeInfo);
    }

    // Q3 - Top 5 Users who fulfilled all the events (view,add,remove,click)
    public static DataSet<Tuple1<Long>> executeQ3(BatchTableEnvironment tEnv) {
        Table table = tEnv.sqlQuery(
                "SELECT userId " +
                "FROM " +
                "(" +
                "SELECT userId " +
                "FROM ClickStream " +
                "GROUP BY userId, eventName" +
                ") " +
                "GROUP BY userId " +
                "HAVING COUNT(*) >=4 " +
                "ORDER BY userId DESC " +
                "LIMIT 5");

        TupleTypeInfo<Tuple1<Long>> tupleTypeInfo = new TupleTypeInfo<>(Types.LONG);
        return tEnv.toDataSet(table, tupleTypeInfo);
    }

    // Q4 - All events of #UserId : 47
    public static DataSet<Tuple2<String, Long>> executeQ4(BatchTableEnvironment tEnv) {
        Table table = tEnv.sqlQuery("SELECT eventName, count(*) AS countEvent FROM ClickStream WHERE userId = 47 GROUP BY eventName");
        TupleTypeInfo<Tuple2<String, Long>> tupleTypeInfo = new TupleTypeInfo<>(Types.STRING, Types.LONG);
        return tEnv.toDataSet(table, tupleTypeInfo);
    }

    // Q5 - Product Views of #UserId : 47
    public static DataSet<Tuple1<Long>> executeQ5(BatchTableEnvironment tEnv) {
        Table table = tEnv.sqlQuery("SELECT count(*) AS countProductView FROM ClickStream WHERE eventName = 'view' GROUP BY productId");
        TupleTypeInfo<Tuple1<Long>> tupleTypeInfo = new TupleTypeInfo<>(Types.LONG);
        return tEnv.toDataSet(table, tupleTypeInfo);
    }

    private static void saveAsCsv(DataSet<?> dataSet,
                          String dataDir,
                          String resultFileName) {
        String outputFileName = String.format(FORMAT_OF_FILE_PATH, dataDir, "output_v2", resultFileName);
        dataSet
                .writeAsCsv(outputFileName, "\n", FIELD_DELIM, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(NUM_FILES);
    }


    private static DataSet<ClickStreamData> createClickStreamDataSet(ExecutionEnvironment env, String filename) {
        return env
                .readCsvFile(filename)
                .fieldDelimiter(FIELD_DELIM)
                .ignoreFirstLine()
                .pojoType(ClickStreamData.class, "date", "productId", "eventName", "userId");
    }


}

package com.dataengineering;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;

public class ClickStreamAnalysis {

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

        // Q1 - Unique Product View counts by ProductId
        createQueryExecutionPlan(dataDir,
                tEnv,
                "SELECT productId, count(*) AS countProductView FROM ClickStream WHERE eventName = 'view' GROUP BY productId",
                new String[]{"productId", "countProductView"},
                new TypeInformation[]{Types.LONG, Types.LONG},
                "q1"
        );

        // Q2 - Unique Event counts
        createQueryExecutionPlan(dataDir,
                tEnv,
                "SELECT eventName, count(*) AS countEvent FROM ClickStream GROUP BY eventName",
                new String[]{"eventName", "countEvent"},
                new TypeInformation[]{Types.STRING, Types.LONG},
                "q2"
        );


        // Q4 - All events of #UserId : 47
        createQueryExecutionPlan(dataDir,
                tEnv,
                "SELECT eventName, count(*) AS countEvent FROM ClickStream WHERE userId = 47 GROUP BY eventName",
                new String[]{"eventName", "countEvent"},
                new TypeInformation[]{Types.STRING, Types.LONG},
                "q4"
        );


        // Q5 - Product Views of #UserId : 47
        createQueryExecutionPlan(dataDir,
                tEnv,
                "SELECT count(*) AS countProductView FROM ClickStream WHERE eventName = 'view' GROUP BY productId",
                new String[]{"countProductView"},
                new TypeInformation[]{Types.LONG},
                "q5"
        );

        // Q3 - Top 5 Users who fulfilled all the events (view,add,remove,click)
        createQueryExecutionPlan(
                dataDir,
                tEnv,
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
                        "LIMIT 5",
                new String[]{"userId"},
                new TypeInformation[]{Types.LONG},
                "q3"
        );

        JobExecutionResult res = env.execute(String.format(FORMAT_OF_EXECUTION_NAME, ClickStreamAnalysis.class.getCanonicalName()));
    }

    private static void createQueryExecutionPlan(String dataDir,
            BatchTableEnvironment tEnv,
                                                 String query,
                                                 String[] outputFieldNames,
                                                 TypeInformation[] outFieldTypes,
                                                 String tableSinkName) {

        String outputFileName = String.format(FORMAT_OF_FILE_PATH, dataDir, "output", tableSinkName);

        Table table = tEnv.sqlQuery(query);
        TableSink csvSink = new CsvTableSink(outputFileName,
                FIELD_DELIM,
                NUM_FILES,
                FileSystem.WriteMode.OVERWRITE)
                .configure(outputFieldNames, outFieldTypes);

        tEnv.registerTableSink(tableSinkName, csvSink);
        table.insertInto(tableSinkName);
    }


    private static DataSet<ClickStreamData> createClickStreamDataSet(ExecutionEnvironment env, String filename) {
        return env
                .readCsvFile(filename)
                .fieldDelimiter(FIELD_DELIM)
                .ignoreFirstLine()
                .pojoType(ClickStreamData.class, "date", "productId", "eventName", "userId");
    }


}

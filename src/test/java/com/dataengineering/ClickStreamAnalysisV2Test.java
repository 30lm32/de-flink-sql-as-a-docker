package com.dataengineering;

import io.flinkspector.core.Order;
import io.flinkspector.core.collection.ExpectedRecords;
import io.flinkspector.core.quantify.MatchTuples;
import io.flinkspector.core.quantify.OutputMatcher;
import io.flinkspector.dataset.DataSetTestBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.core.AllOf.allOf;

public class ClickStreamAnalysisV2Test extends DataSetTestBase {

    private BatchTableEnvironment tEnv;

    @Before
    public void init() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        tEnv = BatchTableEnvironment.create(env);
    }

    // SELECT productId, count(*) AS countProductView FROM ClickStream WHERE eventName = 'view' GROUP BY productId
    @Test
    public void testQ1() {

        DataSet<ClickStreamData> testDataSet =
                createTestDataSetWith(ClickStreamData.of(0L, 10L, "view", 0L))
                        .emit(ClickStreamData.of(1L, 11L, "view", 0L))
                        .emit(ClickStreamData.of(2L, 22L, "view", 0L))
                        .emit(ClickStreamData.of(3L, 11L, "click", 0L))
                        .emit(ClickStreamData.of(4L, 11L, "view", 0L))
                        .emit(ClickStreamData.of(5L, 11L, "remove", 0L))
                        .emit(ClickStreamData.of(6L, 11L, "view", 0L))
                        .close();

        ExpectedRecords<Tuple2<Long, Long>> expectedRecords = ExpectedRecords
                .create(Tuple2.of(10L, 1L))
                .expect(Tuple2.of(11L, 3L))
                .expect(Tuple2.of(22L, 1L));
        expectedRecords.refine().only().inOrder(Order.NONSTRICT);

        OutputMatcher<Tuple2<Long, Long>> matcher =
                new MatchTuples<Tuple2<Long, Long>>("productId", "countProductView")
                        .assertThat("productId", isA(Long.class))
                        .assertThat("countProductView", isA(Long.class))
                        .assertThat("countProductView", greaterThan(0L))
                        .onExactlyNRecords(3);


        tEnv.registerDataSet("ClickStream", testDataSet);
        DataSet<Tuple2<Long, Long>> dataSetActual = ClickStreamAnalysisV2.executeQ1(tEnv);

        assertDataSet(dataSetActual, allOf(expectedRecords, matcher));
        assertDataSet(dataSetActual, expectedRecords);
    }


    // SELECT eventName, count(*) AS countEvent FROM ClickStream GROUP BY eventName
    @Test
    public void testQ2() {

        DataSet<ClickStreamData> testDataSet =
                createTestDataSetWith(ClickStreamData.of(0L, 10L, "view", 0L))
                        .emit(ClickStreamData.of(1L, 11L, "view", 0L))
                        .emit(ClickStreamData.of(2L, 22L, "view", 0L))
                        .emit(ClickStreamData.of(3L, 11L, "click", 0L))
                        .emit(ClickStreamData.of(4L, 11L, "view", 0L))
                        .emit(ClickStreamData.of(5L, 11L, "remove", 0L))
                        .emit(ClickStreamData.of(6L, 11L, "view", 0L))
                        .emit(ClickStreamData.of(7L, 33L, "add", 0L))
                        .close();

        ExpectedRecords<Tuple2<String, Long>> expectedRecords = ExpectedRecords
                .create(Tuple2.of("view", 5L))
                .expect(Tuple2.of("remove", 1L))
                .expect(Tuple2.of("add", 1L))
                .expect(Tuple2.of("click", 1L));

        expectedRecords.refine().only().inOrder(Order.NONSTRICT);


        OutputMatcher<Tuple2<String, Long>> matcher =
                new MatchTuples<Tuple2<String, Long>>("eventName", "countEvent")
                        .assertThat("eventName", isA(String.class))
                        .assertThat("countEvent", isA(Long.class))
                        .assertThat("countEvent", greaterThan(0L))
                        .onExactlyNRecords(4);

        tEnv.registerDataSet("ClickStream", testDataSet);
        DataSet<Tuple2<String, Long>> dataSetActual = ClickStreamAnalysisV2.executeQ2(tEnv);

        assertDataSet(dataSetActual, allOf(expectedRecords, matcher));
        assertDataSet(dataSetActual, expectedRecords);
    }

    // SELECT userId FROM (SELECT userId FROM ClickStream GROUP BY userId, eventName)
    // GROUP BY userId
    // HAVING COUNT(*) >=4
    // ORDER BY userId
    // DESC LIMIT 5
    @Test
    public void testQ3() {

        DataSet<ClickStreamData> testDataSet =
                createTestDataSetWith(ClickStreamData.of(0L, 22L, "view", 22L))
                        .emit(ClickStreamData.of(1L, 11L, "view", 11L))
                        .emit(ClickStreamData.of(2L, 22L, "add", 22L))
                        .emit(ClickStreamData.of(3L, 11L, "click", 11L))
                        .emit(ClickStreamData.of(4L, 11L, "add", 11L))
                        .emit(ClickStreamData.of(5L, 11L, "remove", 11L))
                        .emit(ClickStreamData.of(6L, 11L, "view", 11L))
                        .emit(ClickStreamData.of(7L, 22L, "click", 22L))
                        .emit(ClickStreamData.of(8L, 22L, "click", 22L))
                        .close();

        ExpectedRecords<Tuple1<Long>> expectedRecords = ExpectedRecords
                .create(Tuple1.of(11L));

        expectedRecords.refine().only().inOrder(Order.STRICT);

        OutputMatcher<Tuple1<Long>> matcher =
                new MatchTuples<Tuple1<Long>>("userId")
                        .assertThat("userId", isA(Long.class))
                        .assertThat("userId", greaterThan(0L))
                        .onExactlyNRecords(1);

        tEnv.registerDataSet("ClickStream", testDataSet);
        DataSet<Tuple1<Long>> dataSetActual = ClickStreamAnalysisV2.executeQ3(tEnv);

        assertDataSet(dataSetActual, allOf(expectedRecords, matcher));
        assertDataSet(dataSetActual, expectedRecords);
    }

    // SELECT eventName, count(*) AS countEvent FROM ClickStream WHERE userId = 47 GROUP BY eventName
    @Test
    public void testQ4() {

        DataSet<ClickStreamData> testDataSet =
                createTestDataSetWith(ClickStreamData.of(0L, 10L, "view", 0L))
                        .emit(ClickStreamData.of(1L, 11L, "view", 47L))
                        .emit(ClickStreamData.of(2L, 22L, "view", 47L))
                        .emit(ClickStreamData.of(3L, 11L, "click", 47L))
                        .emit(ClickStreamData.of(4L, 11L, "view", 0L))
                        .emit(ClickStreamData.of(5L, 11L, "remove", 47L))
                        .emit(ClickStreamData.of(6L, 11L, "view", 0L))
                        .emit(ClickStreamData.of(7L, 33L, "add", 47L))
                        .close();

        ExpectedRecords<Tuple2<String, Long>> expectedRecords = ExpectedRecords
                .create(Tuple2.of("view", 2L))
                .expect(Tuple2.of("remove", 1L))
                .expect(Tuple2.of("add", 1L))
                .expect(Tuple2.of("click", 1L));

        expectedRecords.refine().only().inOrder(Order.NONSTRICT);

        OutputMatcher<Tuple2<String, Long>> matcher =
                new MatchTuples<Tuple2<String, Long>>("eventName", "countEvent")
                        .assertThat("eventName", isA(String.class))
                        .assertThat("countEvent", isA(Long.class))
                        .assertThat("countEvent", greaterThan(0L))
                        .onExactlyNRecords(4);

        tEnv.registerDataSet("ClickStream", testDataSet);
        DataSet<Tuple2<String, Long>> dataSetActual = ClickStreamAnalysisV2.executeQ4(tEnv);

        assertDataSet(dataSetActual, allOf(expectedRecords, matcher));
        assertDataSet(dataSetActual, expectedRecords);
    }

    // SELECT count(*) AS countProductView FROM ClickStream WHERE eventName = 'view' GROUP BY productId
    @Test
    public void testQ5() {

        DataSet<ClickStreamData> testDataSet =
                createTestDataSetWith(ClickStreamData.of(0L, 22L, "view", 22L))
                        .emit(ClickStreamData.of(1L, 11L, "view", 11L))
                        .emit(ClickStreamData.of(2L, 22L, "add", 22L))
                        .emit(ClickStreamData.of(3L, 11L, "click", 11L))
                        .emit(ClickStreamData.of(4L, 11L, "add", 11L))
                        .emit(ClickStreamData.of(5L, 11L, "remove", 11L))
                        .emit(ClickStreamData.of(6L, 11L, "view", 11L))
                        .emit(ClickStreamData.of(7L, 22L, "click", 22L))
                        .emit(ClickStreamData.of(8L, 22L, "click", 22L))
                        .close();

        ExpectedRecords<Tuple1<Long>> expectedRecords = ExpectedRecords
                .create(Tuple1.of(2L))
                .expect(Tuple1.of(1L));

        expectedRecords.refine().only().inOrder(Order.NONSTRICT);

        OutputMatcher<Tuple1<Long>> matcher =
                new MatchTuples<Tuple1<Long>>("countProductView")
                        .assertThat("countProductView", isA(Long.class))
                        .assertThat("countProductView", greaterThan(0L))
                        .onExactlyNRecords(2);

        tEnv.registerDataSet("ClickStream", testDataSet);
        DataSet<Tuple1<Long>> dataSetActual = ClickStreamAnalysisV2.executeQ5(tEnv);

        assertDataSet(dataSetActual, allOf(expectedRecords, matcher));
        assertDataSet(dataSetActual, expectedRecords);
    }
}

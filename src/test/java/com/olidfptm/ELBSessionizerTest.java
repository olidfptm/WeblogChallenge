package com.olidfptm;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import static org.junit.Assert.assertTrue;

public class ELBSessionizerTest {

    Logger logger = Logger.getLogger("ELBSessionizerTest");

    private static SparkSession session = SparkSession.builder().master("local[*]")
            .appName("ELBSessionizerTest")
            .getOrCreate();

    static ELBSessionizer snzr = ELBSessionizer.getInstance();

    @Test
    public void testSessionizeByIP(){
        ELBRecord r1 = new ELBRecord();
        r1.setRequestIp("IP1");
        r1.setRequestTimestamp("2015-07-22T09:00:00.000000Z");

        ELBRecord r2 = new ELBRecord();
        r2.setRequestIp("IP1");
        r2.setRequestTimestamp("2015-07-22T09:07:00.000000Z");

        ELBRecord r3 = new ELBRecord();
        r3.setRequestIp("IP1");
        r3.setRequestTimestamp("2015-07-22T09:16:00.000000Z");

        ELBRecord r4 = new ELBRecord();
        r4.setRequestIp("IP1");
        r4.setRequestTimestamp("2015-07-22T09:29:00.000000Z");

        ELBRecord r5 = new ELBRecord();
        r5.setRequestIp("IP1");
        r5.setRequestTimestamp("2015-07-22T09:35:00.000000Z");

        ELBRecord r6 = new ELBRecord();
        r6.setRequestIp("IP2");
        r6.setRequestTimestamp("2015-07-22T09:29:00.000000Z");

        ELBRecord r7 = new ELBRecord();
        r7.setRequestIp("IP2");
        r7.setRequestTimestamp("2015-07-22T09:35:00.000000Z");


        Dataset<Row> dsIn = (Dataset<Row>) session.createDataset(
                Arrays.asList(r1,r2,r3,r4,r5,r6,r7),
                Encoders.bean(ELBRecord.class)).select("*");

        Dataset<Row> ds = snzr.sessionizeByIp(dsIn, 15);

        //input dataset and op dataset size should match
        List<Row> lst = ds.collectAsList();
        assertTrue(lst.size() == 7);
        ds.select("sessionId").show();
        //expecting 4 distinct sessions
        lst = ds.select("sessionId").distinct().collectAsList();
        assertTrue(lst.size() == 4);
    }

    public static class MockSession implements Serializable{
        int sessionId;
        double cumulativeTime;
        String url;
        String requestIp;

        public String getRequestIp() {
            return requestIp;
        }

        public void setRequestIp(String requestIp) {
            this.requestIp = requestIp;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public int getSessionId() {
            return sessionId;
        }

        public void setSessionId(int sessionId) {
            this.sessionId = sessionId;
        }

        public double getCumulativeTime() {
            return cumulativeTime;
        }

        public void setCumulativeTime(double cumulativeTime) {
            this.cumulativeTime = cumulativeTime;
        }
    }
    @Test
    public void testCalcAvgSessionTime(){

        MockSession s1 = new MockSession();
        s1.setSessionId(1);
        s1.setCumulativeTime(0d);

        MockSession s2 = new MockSession();
        s2.setSessionId(1);
        s2.setCumulativeTime(12d);

        MockSession s3 = new MockSession();
        s3.setSessionId(1);
        s3.setCumulativeTime(14d);

        MockSession s4 = new MockSession();
        s4.setSessionId(2);
        s4.setCumulativeTime(0d);

        MockSession s5 = new MockSession();
        s5.setSessionId(2);
        s5.setCumulativeTime(12d);

        Dataset<Row> dsIn = (Dataset<Row>) session.createDataset(
                Arrays.asList(s1,s2,s3,s4,s5),
                Encoders.bean(MockSession.class)).select("*");

        Dataset<Row> ds = snzr.calcAvgSessionTime(dsIn);

        ds.show();

        List<Row> lst = ds.collectAsList();
        assertTrue(lst.size() == 1);

        Row row = lst.get(0);

        assertTrue(((Double)row.getAs("totalSessionLength")).equals(26d) );
        assertTrue(((long)row.getAs("noOfSessions")) == 2L );
        assertTrue(((Double)row.getAs("averageSessionTime")).equals(13d) );
    }

    @Test
    public void testCalcUniqueUrlHits(){

        MockSession s1 = new MockSession();
        s1.setSessionId(1);
        s1.setUrl("U1");

        MockSession s2 = new MockSession();
        s2.setSessionId(1);
        s2.setUrl("U1");

        MockSession s3 = new MockSession();
        s3.setSessionId(1);
        s3.setUrl("U2");

        MockSession s4 = new MockSession();
        s4.setSessionId(2);
        s4.setUrl("U3");

        MockSession s5 = new MockSession();
        s5.setSessionId(2);
        s5.setUrl("U3");

        Dataset<Row> dsIn = (Dataset<Row>) session.createDataset(
                Arrays.asList(s1,s2,s3,s4,s5),
                Encoders.bean(MockSession.class)).select("*");

        Dataset<Row> ds = snzr.calcUniqueUrlHits(dsIn);

        ds.show();

        List<Row> lst = ds.collectAsList();
        assertTrue(lst.size() == 2);

        Row row = lst.get(0);
        assertTrue(((long)row.getAs("uniqueHits")) == 2L );

        row = lst.get(1);
        assertTrue(((long)row.getAs("uniqueHits")) == 1L );
    }

    @Test
    public void testCalcMostEngagedUsers(){

        MockSession s1 = new MockSession();
        s1.setSessionId(1);
        s1.setCumulativeTime(10d);
        s1.setRequestIp("IP1");

        MockSession s2 = new MockSession();
        s2.setSessionId(2);
        s2.setCumulativeTime(14d);
        s2.setRequestIp("IP1");

        MockSession s3 = new MockSession();
        s3.setSessionId(3);
        s3.setCumulativeTime(12d);
        s3.setRequestIp("IP1");

        MockSession s4 = new MockSession();
        s4.setSessionId(4);
        s4.setCumulativeTime(2d);
        s4.setRequestIp("IP2");

        MockSession s5 = new MockSession();
        s5.setSessionId(5);
        s5.setCumulativeTime(0d);
        s5.setRequestIp("IP2");

        Dataset<Row> dsIn = (Dataset<Row>) session.createDataset(
                Arrays.asList(s1,s2,s3,s4,s5),
                Encoders.bean(MockSession.class)).select("*");

        Dataset<Row> ds = snzr.calcMostEngagedUsers(dsIn,2);

        ds.show();

        List<Row> lst = ds.collectAsList();
        assertTrue(lst.size() == 2);

        Row row = lst.get(0);
        assertTrue(((double)row.getAs("longestSessionTime")) == 14d );

        row = lst.get(1);
        assertTrue(((double)row.getAs("longestSessionTime")) == 2d );
    }
}

package com.olidfptm;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ELBAnalyzerApp {

    public static void main(String[] args){

        SparkSession session = SparkSession.builder()
                .master("local[*]")
                .appName("ELBSessionizer")
                .getOrCreate();

        String path = "./data/500KB/50k_aa";

        /*
            This will be a single task since the input is a gz file.
            The program wont work if size exceeds allocated memory.
            An alternative approach would be to uncompress the file
            and split it using other options such as unix scripts.
        */
        Dataset<String> dsRaw = session.read().textFile(path);
        /*
            choosing 2 as a starting point - only way to figure out
            optimal number of partitions is to increment and retest
        */
        dsRaw = dsRaw.repartition(session.sparkContext().defaultParallelism() * 2);

        /*
            The Business Logic bean for sessionization
         */
        ELBSessionizer snzr = ELBSessionizer.getInstance();

        /*
            Parse raw log records
         */
        Dataset<Row> ds  = snzr.parse(dsRaw);

        /*
            1) Sessionize the web log by IP.
         */
        ds = snzr.sessionizeByIp(ds, 15);

        /*
            Considerations - checkpointing needs to be done
            now . The following reports
            all use the sessionized dataset
         */

        /*
            2) Determine the average session time
         */
        snzr.calcAvgSessionTime(ds).show(1,false);

        /*
            3) Determine unique URL visits per session.
            To clarify, count a hit to a unique URL only once per session.
         */
        snzr.calcUniqueUrlHits(ds).show(10,false);

        /*
            4) Find the most engaged users, ie the IPs with the longest session times
         */
        snzr.calcMostEngagedUsers(ds,5).show(10,false);

        //stop the session
        session.stop();
    }
}

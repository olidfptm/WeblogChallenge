package com.olidfptm;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class ELBSessionizer{

    private static class ELBSessionizerHolder{
        private static ELBSessionizer INSTANCE = new ELBSessionizer();
    }

    private ELBSessionizer(){

    }

    public static ELBSessionizer getInstance(){
        return ELBSessionizerHolder.INSTANCE;
    }

    public Dataset<Row> parse(Dataset<String> dsRaw){
        return (Dataset<Row>) dsRaw.map(
                (MapFunction<String, ELBRecord>) s -> ELBRecord.parseRow(s),
                Encoders.bean(ELBRecord.class)
        ).select(col("*"));
    }

    public Dataset<Row> sessionizeByIp(Dataset<Row> dsParsed, int threshHold) {

        //refer https://en.wikipedia.org/wiki/Session_(web_analytics)#Time-oriented_approaches

        WindowSpec specForLag = Window.partitionBy(col("requestIp"))
                .orderBy(col("requestTimestamp"));

        WindowSpec specForSessionId = Window
                .orderBy(col("requestIp"),col("requestTimestamp"),col("rowNum"));

        Column prevValue = lag(col("requestTimestamp"),1).over(specForLag);
        Column timeDiffInMin = getUts(col("requestTimestamp")).minus(getUts(prevValue)).divide(60);
        Column prevCumValue = lag(col("cumulativeTime"),1).over(specForSessionId);

        Dataset<Row> dsRow =
                dsParsed.withColumn("rowNum",
                        row_number().over(specForLag)
                ).withColumn("cumulativeTime",
                        sum(
                                when(timeDiffInMin.isNull(),0).otherwise(timeDiffInMin)
                        ).over(specForLag)
                        .mod(threshHold)
                ).withColumn("sessionFlag",
                        when(prevCumValue.gt(col("cumulativeTime")),1)
                                .otherwise(lit(0))
                ).withColumn("sessionId",
                        sum(col("sessionFlag")).over(specForSessionId)
                );

        return dsRow;
    }

    private static Column getUts(Column string){
        return unix_timestamp(string,"yyyy-MM-dd'T'HH:mm:ss").cast(DataTypes.LongType);
    }

    public Dataset<Row> calcAvgSessionTime(Dataset<Row> dsSessionizedByIp) {

        return dsSessionizedByIp.groupBy(
            col("sessionId")
        ).agg(
            max(col("cumulativeTime")).as("sessionLength")
        ).select(
                sum(col("sessionLength")).as("totalSessionLength"),
                count("sessionId").as("noOfSessions"),
                sum(col("sessionLength")).divide(count("sessionId")).as("averageSessionTime")
        );
    }

    public Dataset<Row> calcUniqueUrlHits(Dataset<Row> dsSessionizedByIp) {

        return dsSessionizedByIp.withColumn("urlRowNumber",
                row_number().over(
                    Window.partitionBy(col("sessionId"),col("url"))
                        .orderBy(col("sessionId"))
                )
        ).filter(
                col("urlRowNumber").equalTo(1)
        ).groupBy(
                col("sessionId")
        ).agg(
                sum(col("urlRowNumber")).as("uniqueHits")
        );

    }

    public Dataset<Row> calcMostEngagedUsers(Dataset<Row> dsSessionizedByIp,int n) {

        return dsSessionizedByIp.groupBy(
                col("requestIp"),col("sessionId")
        ).agg(
                max(col("cumulativeTime")).as("sessionLength")

        ).withColumn("longestSessionTime",
                max(col("sessionLength")).over(
                        Window.partitionBy(
                                col("requestIp")
                        )
                )
        ).withColumn("topNRows",
                row_number().over(
                        Window.partitionBy(
                                col("requestIp")
                        ).orderBy(col("longestSessionTime").desc())
                )
        ).filter(
                col("topNRows").equalTo(1)
        ).limit(n);

    }

}

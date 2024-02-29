import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;

import javax.xml.crypto.Data;

public class texas {
    public  static void  main(String [] args){

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();


        Dataset<Row> phuCases = spark.read()
                .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("sep", ",")
                .option("inferSchema", true)
                .option("header", true)
                .load("D://Online Projects/BigDataAnalyticsFinalProject/src/main/resources/cases_by_status_and_phu.csv")
        .withColumn("FILE_DATE1",functions.to_date(functions.col("FILE_DATE").cast("STRING"),"yyyyMMdd"));
        phuCases.show();
//          phu.printSchema();

        Dataset<Row> phuLocation = spark.read()
                .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("sep", ",")
                .option("inferSchema", true)
                .option("header", true)
                .load("D://Online Projects/BigDataAnalyticsFinalProject/src/main/resources/phu_locations.csv");

        phuLocation.show();
//        phuLocation.printSchema();


//        QuestionNO 1
//        phuLocation.select(functions.col("online_appointments")).show(20,false);
//        phuLocation.select(functions.col("PHU"),functions.col("location_name").alias("location")).
//            where("online_appointments IS NOT NULL").show(20,false);

//        QuestionNO 2
//        phuLocation.select(functions.col("PHU"),functions.col("location_name").alias("location"),
//                        functions.col("city")).
//                where("children_under_2 = 'Yes' AND city LIKE 'Brampton'").show(20,false);

////       QuestionNO 3
//                phuLocation.select(functions.col("PHU"),functions.col("location_name").alias("location"),
//                        functions.col("city")).
//                        where(functions.col("city").equalTo("Toronto")
//                                .and(functions.col("free_parking").equalTo("Yes"))
//                                .and(functions.col("drive_through").equalTo("Yes")))
//                        .show(20,false);

////        QuestionNO 4
//        phuLocation.select(functions.col("friday"),functions.col("PHU"),functions.col("City"))
//                .where(functions.col("friday").isNull())
//                .groupBy(functions.col("City")).agg(functions.count("PHU").alias("ClosePHU"))
//                .show(10,false);


////        QuestionNO 5
//                phuLocation.select(functions.col("PHU"),functions.col("location_name").alias("location"),
//                        functions.col("city")).
//                        where(functions.col("city").equalTo("Toronto").and(functions.col("monday").like("20:00%")
//                                .or(functions.col("monday").like("21:00%"))
//                                )).show(20,false);




////        QuestionNO 6
//        phuLocation.select(functions.col("PHU"),
//                        functions.regexp_replace(functions.col("Monday"),"-"," to ").alias("Monday"),
//                        functions.regexp_replace(functions.col("Tuesday"),"-"," to ").alias("Tuesday"),
//                        functions.regexp_replace(functions.col("Wednesday"),"-"," to ").alias("Wednesday"),
//                        functions.regexp_replace(functions.col("Thursday"),"-"," to ").alias("Thursday"),
//                        functions.regexp_replace(functions.col("Friday"),"-"," to ").alias("Friday"),
//                        functions.regexp_replace(functions.col("Saturday"),"-"," to ").alias("Saturday"),
//                        functions.regexp_replace(functions.col("Saturday"),"-"," to ").alias("Saturday"))
//                 .where(functions.col("city").equalTo("Toronto"))
//                 .show(10,false);


////        QuestionNO 7
//             phuLocation.where(functions.col("temporarily_closed").equalTo("Yes"))
//                     .groupBy(functions.col("city"))
//                     .agg(functions.count("PHU").alias("total"))
////                     .orderBy(functions.col("total").desc())
//                     .show(20,false);


////        QuestionNO 8
//            Dataset<Row> Phone = phuLocation.select(functions.col("phone"))
//                                            .withColumn("Area Code",functions.element_at(functions.split(functions.col("phone"),"-"),1))
//                                             .withColumn("Extension",functions.element_at(functions.split(functions.col("phone"),"ext."),2));
//                         Phone.show(20,false);


////        QuestionNO 9

//          phuCases.show();
//          phuCases.where("FILE_DATE BETWEEN '20200401' AND '20200930'")
//                  .groupBy(functions.col("PHU_NAME"))
//                  .agg(functions.max(functions.col("ACTIVE_CASES")).alias("ACTIVE_CASES"))
//                  .orderBy(functions.col("ACTIVE_CASES").asc())
//                  .show(10,false);


////        QuestionNO 10
//           phuCases.select(functions.col("PHU_NAME"),
//                           functions.col("RESOLVED_CASES"),
//                           functions.col("ACTIVE_CASES"),
//                           functions.to_date(functions.col("FILE_DATE").cast("STRING"),"yyyyMMdd").alias("FILE_DATE"))
//                   .where("RESOLVED_CASES > ACTIVE_CASES AND CAST(FILE_DATE AS STRING) LIKE '2020%'")
//                   .show(10,false);
//        query2='SELECT DISTINCT PHU_NAME,MONTH(FILE_DATE) AS MONTH, YEAR(FILE_DATE) AS YEAR  FROM cases WHERE DEATHS =
//        (SELECT MIN(DEATHS) FROM cases) group by PHU_NAME, MONTH(FILE_DATE), YEAR(FILE_DATE)'





////        QuestionNO 11


//        Dataset<Row> qust11a= phuCases.withColumn("PHU_NAME",functions.col("PHU_NAME")).distinct()
//                .withColumn("Month",functions.month(functions.col("FILE_DATE1")))
//                .withColumn("Year",functions.year(functions.col("FILE_DATE1")))
//                .groupBy(functions.col("PHU_NAME"),functions.col("Month"),functions.col("Year"))
//                .agg(functions.min(functions.col("DEATHS")))
//                ;
//
//        qust11a.show();






////          Question 12
//        phuCases.groupBy(functions.col("PHU_NAME"))
//                .agg(functions.sum("ACTIVE_CASES"))
//                .orderBy(functions.sum(functions.col("ACTIVE_CASES")).asc())
//                .show(20,false);


////          Question 13
//        Dataset<Row> Question13 = phuCases.where(functions.col("PHU_NAME").like("NIAGARA REGION"))
//                .groupBy(functions.col("FILE_DATE"))
//                .agg(functions.max("DEATHS").alias("Maxdeaths"));
//        Question13  = Question13.where(functions.col("Maxdeaths").isNotNull());
//        Question13.orderBy(functions.col("Maxdeaths").desc()).show(1,false);


        ////          Question 14
//        query2='SELECT DISTINCT PHU_NAME, SUM(RESOLVED_CASES) AS TOTAL_RESOLVED_CASES from cases where MONTH(FILE_DATE) IN (5,10) AND YEAR(FILE_DATE)=2020 group by PHU_NAME'

//        phuCases.withColumn("PHU_NAME",functions.col("PHU_NAME")).distinct()
//                .withColumn("Month",functions.month(functions.col("FILE_DATE1")))
//                .withColumn("Year",functions.year(functions.col("FILE_DATE1")))
//                .where(functions.col("Month").between("5","10")
//                          .and(functions.col("Year").equalTo("2020")))
//                .groupBy(functions.col("PHU_NAME"))
//                .agg(functions.sum(functions.col("RESOLVED_CASES")).alias("TOTAL_RESOLVED_CASES"))
//                .show(30,false);


////          Question 15

//        Dataset<Row> percentage = phuCases
//                .withColumn("fad",functions.col("PHU_NAME")).distinct()
////        .withColumn("perc", functions.round ( ( functions.col("ACTIVE_CASES").divide(functions.sum(functions.col("RESOLVED_CASES")).over()) )
////                .multiply(100) ,2))
//                .groupBy(functions.col("PHU_NAME"))
//                .agg(functions.round(( functions.col("ACTIVE_CASES").divide(functions.sum(functions.col("RESOLVED_CASES")).over()) )
//                        .multiply(100) ,2));
//        percentage.show();

    }
}

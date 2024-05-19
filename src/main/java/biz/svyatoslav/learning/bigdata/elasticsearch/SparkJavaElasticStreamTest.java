package biz.svyatoslav.learning.bigdata.elasticsearch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;

public class SparkJavaElasticStreamTest {
    public static void main(String[] args) {
        try {
            System.out.println("Connecting to Elasticsearch...");
            SparkSession spark = SparkSession.builder()
                .config(ConfigurationOptions.ES_NODES, "0.0.0.0")
                .config(ConfigurationOptions.ES_PORT, "9200")
                .appName("StreamingElastic")
                .master("local[*]")
                .getOrCreate();

            System.out.println("Preparing simple data...");
            var staticDataFrame = spark.read()
                .option("header", "true")
                .csv("src/main/resources/test")
                .schema();

            Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .schema(staticDataFrame)
                .load("src/main/resources/test");

            String esIndex = "receipt_restaurants/data";

            System.out.println("Writing simple data...");
            df.write()
                .format("org.elasticsearch.spark.sql")
                .option(ConfigurationOptions.ES_RESOURCE, esIndex)
                .mode(SaveMode.Append)
                .save();

            spark.stop();
            System.out.println("Done. Visit http://localhost:9200/receipt_restaurants and http://localhost:9200/receipt_restaurants/_search?pretty to see the result.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
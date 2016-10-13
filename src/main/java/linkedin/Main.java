package linkedin;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

/**
 * Created by Evegeny on 13/10/2016.
 */
public class Main {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\util\\hadoop-common-2.2.0-bin-master\\");
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("linkedIn");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> frame = sqlContext.read().json("data/linkedIn/*.json");
        frame.show();
        frame.printSchema();
        Arrays.stream(frame.schema().fields()).map(StructField::dataType).forEach(System.out::println);
        Row[] collect = (Row[]) frame.collect();
        String[] columns = frame.columns();
        for (Row row : collect) {
            for (String column : columns) {
                System.out.println(row.getAs(column).toString());
            }
        }

        frame = frame.withColumn("salary", col("age").multiply(size(col("keywords"))).multiply(10));
        frame.show();
       /* Row rows = frame.select(explode(col("keywords")).as("keyword")).
                groupBy("keyword").count().sort(col("count").desc()).take(1)[0];
        String mostPopular = rows.getAs("keyword").toString();
        System.out.println("mostPopular = " + mostPopular);

        frame.where(array_contains(col("keywords"),mostPopular).and(col("salary").leq(1200))).show();*/

    }
}

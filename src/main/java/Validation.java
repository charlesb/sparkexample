import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by cb186046 on 17/9/17.
 *
 * mvn install -DskipTests
 * spark-submit --master yarn --deploy-mode client --jars /usr/local/hive/hive-1.1.0-cdh5.8.0/lib/mysql-connector-java-5.1.40-bin.jar,/usr/local/hive/hive-1.1.0-cdh5.8.0/lib/hive-contrib-1.1.0-cdh5.8.0.jar --name sparkExample --class Validation target/spark-example.jar
 * spark-shell --master yarn --deploy-mode client --jars /usr/local/hive/hive-1.1.0-cdh5.8.0/lib/mysql-connector-java-5.1.40-bin.jar,/usr/local/hive/hive-1.1.0-cdh5.8.0/lib/hive-contrib-1.1.0-cdh5.8.0.jar
 *
 */
public class Validation {

    /**
     *
     * @param args
     */
    public static void main(String [] args) {
        SparkConf sparkConf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        HiveContext hiveContext = new org.apache.spark.sql.hive.HiveContext(sc);
//        Row[] results = hiveContext.sql("select current_rating from staging_d01_GB_RMS.eda_RMS_SML_RATING_REPORT_D").collect();
//        System.out.println(results.length);
        Dataset<Row> df = hiveContext.sql("select current_rating from staging_d01_GB_RMS.eda_RMS_SML_RATING_REPORT_D");
        System.out.println(df.count());
//        DataFrame sum = df.select(df.col("current_rating").cast("int")).groupBy().sum();
//        sum.show();
    }

}

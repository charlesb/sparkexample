import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class Java8Exploration {

    public static void main(String [] args) {

        SparkSession spark = SparkSession
            .builder()
              .appName("Spark CSV Reader")
            .config("spark.master", "local")
            .getOrCreate();

        Dataset<Row> df = spark.read().option("header", "true").csv(
            "hdfs://localhost:9000/user/cb186046/new-registration-of-motocycles-by-make.csv"
        );

        df.show();
        df.printSchema();

//        df.select(col("make"), col("number")).write().format("parquet").save(
//            "hdfs://localhost:9000/user/cb186046/motocycles-by-make.parquet"
//        );

        df.filter(col("make").equalTo("APRILIA")).show();


    }
}

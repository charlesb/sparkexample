import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class ETL {

    static final String JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
    static final String DB_URL = "jdbc:oracle:thin:@//localhost:49162/xe";
    static final String USER = "edf";
    static final String PASS = "hadoop";

    public static void main(String [] args) {

//        String test = "D|~|4764|~|MFA_MRA|~|20140731105104|~|MFA_MRA|~|20140731105104|~|1807|~||~||~||~||~||~||~||~|MFA_MRA <2d LC Rest of World>|~|20140731105104|~||~|-|~| 7|~||~|LC Rest Of The World|~||~|UOB|~||~||~||~||~|1373|~||~||~||~|";
//        String[] testArr = test.split(Pattern.quote("|~|"), -1);
//        System.out.println(Arrays.toString(testArr));
//        System.exit(0);

        // YYYYMMDDHHMMSS and 20140731105104
//        String strdatetime = "20140731105104";
//        String format = "yyyyMMddHHmmss";
//        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
//        LocalDateTime datetime = LocalDateTime.parse(strdatetime, formatter);
//        Timestamp timestamp = Timestamp.valueOf(datetime);
//        System.out.println(timestamp);
//        System.exit(0);

//        System.out.println(System.getenv("SPARK_HOME"));
//        System.out.println(System.getenv("CLASSPATH"));

        // Generate the schema based on the metadata
        List<StructField> fields = new ArrayList<>();
        ArrayList<String> columns = new ArrayList<>();
        Map<String, HashMap<String, Object>> metadata = new HashMap<>();

        Connection conn = null;
        Statement stmt;

        try {
            Class.forName(JDBC_DRIVER);
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Creating statement...");
            stmt = conn.createStatement();
            String sql;
            sql = "SELECT FLD_NM, FLD_DATA_TYPE_TXT, FLD_LEN_NUM, FLD_DEC_PREC, FLD_FORMAT_TXT, FLD_OPTIONALITY FROM EDF.EDAG_FIELD_DETAIL WHERE RCRD_TYP_CD IN ('FI', 'PK') ORDER BY FLD_NUM ASC";
            ResultSet rs = stmt.executeQuery(sql);
            while(rs.next()) {
                String FLD_NM = rs.getString("FLD_NM");
                Integer FLD_LEN_NUM = rs.getInt("FLD_LEN_NUM");
                Integer FLD_DEC_PREC = rs.getInt("FLD_DEC_PREC");
                String FLD_DATA_TYPE_TXT = rs.getString("FLD_DATA_TYPE_TXT");
                String FLD_FORMAT_TXT = rs.getString("FLD_FORMAT_TXT");
                String FLD_OPTIONALITY = rs.getString("FLD_OPTIONALITY");
//                System.out.print("FLD_NM: " + FLD_NM);
//                System.out.print(", FLD_LEN_NUM: " + FLD_LEN_NUM);
//                System.out.print(", FLD_DEC_PREC: " + FLD_DEC_PREC);
//                System.out.print(", FLD_DATA_TYPE_TXT: " + FLD_DATA_TYPE_TXT);
//                System.out.print(", FLD_FORMAT_TXT: " + FLD_FORMAT_TXT);
//                System.out.println(", FLD_OPTIONALITY: " + FLD_OPTIONALITY);

                // Map to proper datetime format
                if (FLD_FORMAT_TXT != null) {
                    switch (FLD_FORMAT_TXT) {
                        case "YYYYMMDD":
                            FLD_FORMAT_TXT = "yyyyMMdd";
                            break;
                        case "YYYYMMDDHHMMSS":
                            FLD_FORMAT_TXT = "yyyyMMddHHmmss";
                            break;
                    }
                }

                columns.add(FLD_NM);

                HashMap<String, Object> params = new HashMap<>();
                params.put("FLD_LEN_NUM", FLD_LEN_NUM);
                params.put("FLD_DEC_PREC", FLD_DEC_PREC);
                params.put("FLD_DATA_TYPE_TXT", FLD_DATA_TYPE_TXT);
                params.put("FLD_FORMAT_TXT", FLD_FORMAT_TXT);
                params.put("FLD_OPTIONALITY", FLD_OPTIONALITY);
                metadata.put(FLD_NM, params);

                // define data type
                DataType dt = null;
                switch (FLD_DATA_TYPE_TXT) {
                    case "A":
                        dt = DataTypes.StringType;
                        break;
                    case "N":
                        dt = new DecimalType(FLD_LEN_NUM, FLD_DEC_PREC);
                        break;
                    case "T":
                        dt = DataTypes.TimestampType;
                        break;
                }
                StructField field = DataTypes.createStructField(FLD_NM, dt, FLD_OPTIONALITY.trim().equalsIgnoreCase("O") ? true : false);
                fields.add(field);
            }
            // add 4 more columns proc_instance_id, proc_ts, site_id, biz_dt
            fields.add(DataTypes.createStructField("proc_instance_id", DataTypes.StringType, false));
            fields.add(DataTypes.createStructField("proc_ts", DataTypes.TimestampType, false));
            fields.add(DataTypes.createStructField("site_id", DataTypes.IntegerType, false));
            fields.add(DataTypes.createStructField("biz_dt", DataTypes.DateType, false));

            rs.close();
            stmt.close();
            conn.close();
        } catch (SQLException se) {
            se.printStackTrace();
        } catch (ClassNotFoundException cnfe) {
            cnfe.printStackTrace();
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }

//        SparkSession spark = SparkSession
//            .builder()
//            .appName("T1 to T1.1")
//            .master("local[*]")
//            .config("hive.metastore.uris", "thrift://localhost:9083")
//            .enableHiveSupport()
//            .getOrCreate();

//        spark.sql("SELECT * FROM staging_dev_SG_RMS.eda_RMS_SML_RATING_REPORT_D").show();


//        spark.catalog().listDatabases().show(false);
//
//        try {
//            Table stagingTable = spark.catalog().getTable("staging_dev_SG_RMS", "eda_RMS_SML_RATING_REPORT_D");
//            System.out.println(stagingTable.description());
//        } catch (AnalysisException e) {
//            e.printStackTrace();
//        }

//        String test = "D|~|4764|~|MFA_MRA|~|20140731105104|~|MFA_MRA|~|20140731105104|~|1807";
//        String[] ar = test.split(Pattern.quote("|~|"));
//        System.out.println(Arrays.toString(ar));

        StructType schema = DataTypes.createStructType(fields);

        // Bring the data
        SparkSession spark = SparkSession
            .builder()
            .master("local[*]")
            .appName("T1 to T1.1")
            .getOrCreate();

        JavaRDD<String> rawRDD = spark.read()
            .textFile("hdfs://localhost:9000/user/cb186046/SG/RMS/SML_RATING_REPORT/biz_dt=2012-01-01")
            .toJavaRDD();

//        rawRDD.take(10).forEach(System.out::println);

        String proc_instance_id = "test1";
        Timestamp  proc_ts = Timestamp.valueOf(LocalDateTime.now());
        Integer site_id = 1;
        Date biz_dt = Date.valueOf(LocalDate.of(2012, 1, 1));

        // Convert records of the RDD to Rows
        JavaRDD<Row> rowRDD = rawRDD.map((Function<String, Row>) record -> {
            String[] attributes = record.split(Pattern.quote("|~|"), -1);
            Object[] values = new Object[attributes.length + 4];
            for (int i = 0; i < attributes.length; i++) {
                switch (metadata.get(columns.get(i)).get("FLD_DATA_TYPE_TXT").toString()) {
                    case "A":
                        values[i] = attributes[i].trim();
                        break;
                    case "N":
                        values[i] = new BigDecimal(attributes[i].toString().trim().isEmpty() ? "0" : attributes[i].toString().trim());
                        break;
                    case "T":
                        LocalDateTime datetime;
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(
                            metadata.get(columns.get(i)).get("FLD_FORMAT_TXT").toString()
                        );
                        if (attributes[i].toString().trim().isEmpty()) {
                            if (metadata.get(columns.get(i)).get("FLD_FORMAT_TXT").toString() == "yyyyMMdd") {
                                datetime = LocalDateTime.parse("19800101", formatter);
                                values[i] = Timestamp.valueOf(datetime);
                            }
                            if (metadata.get(columns.get(i)).get("FLD_FORMAT_TXT").toString() == "yyyyMMddHHmmss") {
                                datetime = LocalDateTime.parse("19800101000000", formatter);
                                values[i] = Timestamp.valueOf(datetime);
                            }
                        } else {
                            datetime = LocalDateTime.parse(attributes[i].toString().trim(), formatter);
                            values[i] = Timestamp.valueOf(datetime);
                        }
                        break;
                }
            }
            // add 4 more columns proc_instance_id, proc_ts, site_id and biz_dt
            values[attributes.length] = proc_instance_id;
            values[attributes.length + 1] = proc_ts;
            values[attributes.length + 2] = site_id;
            values[attributes.length + 3] = biz_dt;
            return RowFactory.create(values);
        });

        // Apply the schema to the RDD
        Dataset<Row> df = spark.createDataFrame(rowRDD, schema);

//        df.show(100000, false);

        // Write to HDFS as parquet file
        // Available codecs are uncompressed, gzip, lzo, snappy, none
        df.write()
            .option("compression", "gzip")
//            .partitionBy("site_id", "biz_dt") // it will remove existing partitions
            .mode(SaveMode.Overwrite) // append will add within the partition not replace
            .parquet(String.format(
                "hdfs://localhost:9000/user/cb186046/SG/RMS/SML_RATING_REPORT/gz/site_id=%1$d/biz_dt=%2$tY-%2$tm-%2$td",
                site_id,
                biz_dt
            ));

        spark.close();

    }

}

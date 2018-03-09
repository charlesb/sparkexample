import com.google.common.collect.Lists;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.SparkConf;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by cb186046 on 15/9/17.
 */
public class SparkJobSubmitter {

    private static final Logger logger = LoggerFactory.getLogger(SparkJobSubmitter.class);
    private String jobName;
    private String jobClass;
    private String applicationJar;
    private String[] additionalJars;
    private String[] files;
    private Properties sparkProperties;

    public SparkJobSubmitter(String jobName, String jobClass, String applicationJar,
                             Properties sparkProperties, String[] additionalJars, String[] files) {

        this.jobName = jobName;
        this.jobClass = jobClass;
        this.applicationJar = applicationJar;
        this.sparkProperties = sparkProperties;
        this.additionalJars = additionalJars;
        this.files = files;

    }

    public void submit() {
        List<String> args = Lists.newArrayList("--name", jobName, "--jar", applicationJar, "--class", jobClass);

        if (additionalJars != null && additionalJars.length > 0) {
            args.add("--files");
            args.add(StringUtils.join(files, ","));
        }

        if (sparkProperties.getProperty("spark.executor.cores") != null) {
            args.add("--executor-cores");
            args.add(sparkProperties.getProperty("spark.executor.cores"));
        }

        if (sparkProperties.getProperty("spark.executor.memory") != null) {
            args.add("--executor-memory");
            args.add(sparkProperties.getProperty("spark.executor.memory"));
        }

        if (sparkProperties.getProperty("spark.driver.cores") != null) {
            args.add("--driver-cores");
            args.add(sparkProperties.getProperty("spark.driver.cores"));
        }

        if (sparkProperties.getProperty("spark.driver.memory") != null) {
            args.add("--driver-memory");
            args.add(sparkProperties.getProperty("spark.driver.memory"));
        }

        System.setProperty("SPARK_YARN_MODE", "true");

        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.yarn.preserve.staging.files", "true");

        for (Map.Entry<Object, Object> e : sparkProperties.entrySet()) {
            sparkConf.set(e.getKey().toString(), e.getValue().toString());
        }

        logger.info("Spark args: {}", Arrays.toString(args.toArray()));
        logger.info("Spark conf settings: {}", Arrays.toString(sparkConf.getAll()));

//        ClientArguments cArgs = new ClientArguments(args.toArray(new String[args.size()]), sparkConf);
//        Client client = new Client(cArgs, new Configuration(), sparkConf);
//        client.run();

    }
}

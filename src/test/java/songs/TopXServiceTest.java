package songs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import songs.config.Config;

import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Created by Evegeny on 12/10/2016.
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = Config.class)
@ActiveProfiles("dev")
public class TopXServiceTest {

    @Autowired
    private JavaSparkContext sc;

    @Autowired
    private TopXService topXService;

    @Test
    public void topX() throws Exception {
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("java", "java", "scala"));
        Assert.assertEquals("java",topXService.topX(rdd,1).get(0));
    }

}
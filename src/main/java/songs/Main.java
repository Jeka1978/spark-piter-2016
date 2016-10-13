package songs;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import songs.config.Config;

/**
 * Created by Evegeny on 12/10/2016.
 */
public class Main {
    public static void main(String[] args) throws InterruptedException {
        System.setProperty("hadoop.home.dir", "C:\\util\\hadoop-common-2.2.0-bin-master\\");
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Config.class);
        TopXService service = context.getBean(TopXService.class);
        JavaSparkContext sc = context.getBean(JavaSparkContext.class);
        JavaRDD<String> rdd = sc.textFile("data/songs/beatles/*");
        Thread.sleep(50000);
        service.topX(rdd,3).forEach(System.out::println);
    }
}

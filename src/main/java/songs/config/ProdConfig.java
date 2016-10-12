package songs.config;

import org.apache.spark.SparkConf;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Created by Evegeny on 12/10/2016.
 */
@Configuration
public class ProdConfig {
    @Bean
    @Profile("prod")
    public SparkConf conf(){
        return new SparkConf().setAppName("topX");
    }
}

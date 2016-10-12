package songs;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.List;

/**
 * Created by Evegeny on 12/10/2016.
 */
@Service
public class TopXService {
    public List<String> topX(JavaRDD<String> lines, int x) {
        JavaRDD<String> rdd = lines.map(String::toLowerCase).flatMap(WordsUtil::getWords);
        JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(word -> new Tuple2<>(word, 1));
        List<String> take = pairRDD.reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap).sortByKey(false)
                .map(Tuple2::_2).take(x);

        return take;
    }
}

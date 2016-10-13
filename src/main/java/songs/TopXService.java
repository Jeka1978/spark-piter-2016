package songs;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.List;

/**
 * Created by Evegeny on 12/10/2016.
 */
@Service
public class TopXService implements Serializable{

    @AutowiredBroadcast(UserProps.class)
    private Broadcast<UserProps> userProps;



    public List<String> topX(JavaRDD<String> lines, int x) {
        JavaRDD<String> words = lines.map(String::toLowerCase).flatMap(WordsUtil::getWords);
        words = words.filter(word -> !userProps.value().garbage.contains(word));
        JavaPairRDD<String, Integer> pairRDD = words.mapToPair(word -> new Tuple2<>(word, 1));
        List<String> take = pairRDD.reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap).sortByKey(false)
                .map(Tuple2::_2).take(x);

        return take;
    }
}

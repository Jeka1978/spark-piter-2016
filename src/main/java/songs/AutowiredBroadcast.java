package songs;/**
 * Created by Evegeny on 13/10/2016.
 */

import java.lang.annotation.Retention;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
public @interface AutowiredBroadcast {
    Class value();
}

package songs;

import lombok.SneakyThrows;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;

/**
 * Created by Evegeny on 13/10/2016.
 */
@Component
public class AutowiredBroadcastAnnotationBeanPostProcessor implements BeanPostProcessor {
    @Autowired
    private JavaSparkContext sc;

    @Autowired
    private ApplicationContext context;


    @Override
    @SneakyThrows
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        Field[] fields = bean.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(AutowiredBroadcast.class)) {
                field.setAccessible(true);
                AutowiredBroadcast annotation = field.getAnnotation(AutowiredBroadcast.class);
                Class beanClass = annotation.value();
                Object beanToInject = context.getBean(beanClass);
                Broadcast<Object> broadcast = sc.broadcast(beanToInject);
                field.set(bean,broadcast);
            }
        }
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }
}

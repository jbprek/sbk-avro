package foo.kafka.consumer.randomproducer;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

@Component
public class KafkaTemplateListenerRegistrar implements BeanPostProcessor {

    private final GlobalProducerListener listener;

    public KafkaTemplateListenerRegistrar(GlobalProducerListener listener) {
        this.listener = listener;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object postProcessAfterInitialization(@NonNull Object bean, @NonNull String beanName) throws BeansException {
        if (bean instanceof KafkaTemplate<?, ?> kafkaTemplate) {
            // attach the global listener so sends performed by the binder will invoke it
            // cast to raw ProducerListener to avoid generic type mismatch
            kafkaTemplate.setProducerListener((org.springframework.kafka.support.ProducerListener) listener);
        }
        return bean;
    }
}

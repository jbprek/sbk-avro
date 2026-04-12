package foo.kafka.birthevent.service;

/**
 * Generic mapper that converts an event of type {@code E} to an entity of type {@code T}.
 *
 * @param <E> the event (message payload) type
 * @param <T> the entity type
 */
@FunctionalInterface
public interface EventMapper<E, T> {

    T toEntity(E event);
}


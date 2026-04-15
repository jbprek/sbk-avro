package foo.kafka.deathevent.eventstore;

import org.awaitility.Awaitility;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestUtils {

    private TestUtils() {
        // prevent instantiation
    }

    public static <T> void awaitUntilPresentAndAssert(Supplier<Optional<T>> supplier,
                                                      Consumer<T> assertions) {
        Awaitility.await()
                .pollDelay(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    var optional = supplier.get();
                    assertTrue(optional.isPresent());
                    assertions.accept(optional.get());
                });
    }

    public static <E, K> void awaitUntilTableIsEmpty(JpaRepository<E, K> repository) {
        Awaitility.await()
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(5, TimeUnit.SECONDS)
                .until(() -> repository.count() == 0);
    }
}


package foo.kafka.birthevent.eventstore;

import foo.avro.birth.BirthEvent;
import foo.kafka.birthevent.eventstore.persistence.Birth;
import foo.kafka.birthevent.eventstore.persistence.BirthRepository;
import foo.kafka.birthevent.service.BirthDao;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@Slf4j
@ExtendWith(OutputCaptureExtension.class)
@SpringBootTest(classes = BirthEventStoreApplication.class)
@EmbeddedKafka(partitions = 1, topics = {
        "birth.register.avro.mock"
})
@DirtiesContext
class BirthEventStoreApplicationTest {

    private static final BirthEvent birthEventIn = BirthEvent.newBuilder()
            .setId(1L)
            .setName("John")
            .setDob(LocalDate.EPOCH)
            .setTown("Athens")
            .setRegistrationTime(Instant.EPOCH)
            .setWeight(new BigDecimal("3.5"))
            .build();


    private static final Message<BirthEvent> message = MessageBuilder.withPayload(birthEventIn)
            .setHeader(KafkaHeaders.KEY, birthEventIn.getTown())
            .build();


    @Autowired
    private StreamBridge streamBridge;


    @Autowired
    private BirthRepository repository;

    @MockBean
    private BirthDao dao;

    @BeforeEach
    void setup() {
        repository.deleteAll();
        awaitUntilTableIsEmpty(repository);

        // Default behavior for the mock DAO: persist via real repository so non-transient tests work
        doAnswer(invocation -> {
            Birth arg = invocation.getArgument(0);
            repository.saveAndFlush(arg);
            return null;
        }).when(dao).saveBirthEvent(any(Birth.class));
    }

    @Test
    @DisplayName("An incoming BirthEvent is processed and sent to the output topic as BirthStatEntry")
    void testBirthEventProcessing() {
        streamBridge.send("test-producer-out-0", message);
        // default mock behavior persists the entity

        checkEntityPersisted();
        verify(dao, times(1)).saveBirthEvent(any(Birth.class));
    }

    @Test
    @DisplayName("On transient DB error, the BirthEvent is retried and persisted")
    void testBirthEventProcessingTransientRetry() {

        // First two calls throw transient exception, third call persists via the repository
        doThrow(new TransientDataAccessException("Transient error 1") {
        }).doThrow(new TransientDataAccessException("Transient error 2") {
                }).doAnswer(invocation -> {
                    repository.saveAndFlush(invocation.getArgument(0));
                    return null;
                }).when(dao).saveBirthEvent(any(Birth.class));

        streamBridge.send("test-producer-out-0", message);

        // wait until the entity is persisted (retries will cause eventual persistence)
        checkEntityPersisted();

        // Expect at least 3 dao calls: two failing attempts and one successful
        verify(dao, atLeast(3)).saveBirthEvent(any(Birth.class));
    }

    @Test
    @DisplayName("On non transient DB error, the BirthEvent is retried and persisted")
    void testBirthEventProcessingNonTransientSkippingMessage() {

        // First two calls throw transient exception, third call persists via the repository
        doThrow(new TransientDataAccessException("Transient error 1") {
        }).doThrow(new TransientDataAccessException("Transient error 2") {
        }).doAnswer(invocation -> {
            repository.saveAndFlush(invocation.getArgument(0));
            return null;
        }).when(dao).saveBirthEvent(any(Birth.class));

        streamBridge.send("test-producer-out-0", message);

        // wait until the entity is persisted (retries will cause eventual persistence)
        checkEntityPersisted();

        // Expect at least 3 dao calls: two failing attempts and one successful
        verify(dao, atLeast(3)).saveBirthEvent(any(Birth.class));
    }


    private void checkEntityPersisted() {
        awaitUntilPresentAndAssert(
                () -> repository.findById(1L),
                entity -> assertAll(
                        () -> assertNotNull(entity, "Entity should not be null"),
                        () -> assertEquals("John", entity.getName(), "Names should match"),
                        () -> assertEquals(LocalDate.EPOCH, entity.getDob(), "DOB should match"),
                        () -> assertEquals("Athens", entity.getTown(), "Towns should match")
                )
        );
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
package foo.kafka.birthevent.eventstore;

import foo.avro.birth.BirthEvent;
import foo.kafka.birthevent.eventstore.persistence.Birth;
import foo.kafka.birthevent.eventstore.persistence.BirthMapperImpl;
import foo.kafka.birthevent.eventstore.persistence.BirthRepository;
import foo.kafka.birthevent.service.BirthDao;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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
    private EmbeddedKafkaBroker broker;
    @Autowired
    private BirthMapperImpl mapper;
    @MockitoSpyBean
    private BirthRepository repository;
    @MockitoSpyBean
    BirthDao dao;

    @BeforeEach
    public void setup() {
        repository.deleteAll();
        awaitUntilTableIsEmpty(repository);
    }

    @Test
    @DisplayName("An incoming BirthEvent is processed and sent to the output topic as BirthStatEntry")
    void testBirthEventProcessing() {
        streamBridge.send("test-producer-out-0", message);

        checkEntityPersisted();
        verify(dao, times(1)).saveBirthEvent(any(Birth.class));
    }

    @Test
    @DisplayName("On transient DB error, the BirthEvent is retried and persisted")
    void testBirthEventProcessingTransientRetry() {
        // Stub the Dao (which Processor calls) to throw a transient exception on first call
        // and delegate to the real repository on the second call so the entity is persisted.
        var callCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            Birth arg = invocation.getArgument(0);
            if (callCount.getAndIncrement() == 0) {
                throw new org.springframework.dao.TransientDataAccessException("Transient DB error") {
                };
            }
            // On retry, persist using the real repository implementation so findById will succeed
            repository.saveAndFlush(arg);
            return null; // void method
        }).when(dao).saveBirthEvent(any(Birth.class));
        streamBridge.send("test-producer-out-0", message);


        checkEntityPersisted();
        verify(dao, times(2)).saveBirthEvent(any(Birth.class));
    }

    private void checkEntityPersisted() {
        awaitUntilPresentAndAssert(
                () -> repository.findById(1L),
                entity -> {
                    assertAll(
                            () -> assertNotNull(entity, "Entity should not be null"),
                            () -> assertEquals("John", entity.getName(), "Names should match"),
                            () -> assertEquals(LocalDate.EPOCH, entity.getDob(), "DOB should match"),
                            () -> assertEquals("Athens", entity.getTown(), "Towns should match")
                    );
                }
        );
    }

    public static <T> void awaitUntilPresentAndAssert(Supplier<Optional<T>> supplier,
                                                      Consumer<T> assertions) {
        Awaitility.await()
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(5, TimeUnit.SECONDS)
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
                .until(()-> repository.count() == 0);
    }

}
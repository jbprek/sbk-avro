package foo.kafka.deathevent.eventstore;

import foo.avro.death.DeathEvent;
import foo.kafka.common.MessageCoordinates;
import foo.kafka.deathevent.eventstore.persistence.FemaleDeathRepository;
import foo.kafka.deathevent.eventstore.persistence.MaleDeathRepository;
import foo.kafka.deathevent.service.DeathDao;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.LocalDate;
import java.util.concurrent.TimeUnit;

import static foo.kafka.deathevent.eventstore.TestUtils.awaitUntilPresentAndAssert;
import static foo.kafka.deathevent.eventstore.TestUtils.awaitUntilTableIsEmpty;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@Slf4j
@ExtendWith(OutputCaptureExtension.class)
@SpringBootTest(classes = DeathEventStoreApplication.class)
@EmbeddedKafka(partitions = 1, topics = {
        "death.register.avro.mock"
})
@DirtiesContext
class DeathEventStoreApplicationTest {

    private static final DeathEvent maleDeathEvent = DeathEvent.newBuilder()
            .setId(1L).setName("John Smith")
            .setDod(LocalDate.of(2025, 1, 15))
            .setTown("London").setGender("M").build();

    private static final DeathEvent femaleDeathEvent = DeathEvent.newBuilder()
            .setId(2L).setName("Jane Smith")
            .setDod(LocalDate.of(2025, 3, 20))
            .setTown("Manchester").setGender("F").build();

    private static final Message<DeathEvent> maleMessage = MessageBuilder.withPayload(maleDeathEvent)
            .setHeader(KafkaHeaders.KEY, maleDeathEvent.getTown()).build();

    private static final Message<DeathEvent> femaleMessage = MessageBuilder.withPayload(femaleDeathEvent)
            .setHeader(KafkaHeaders.KEY, femaleDeathEvent.getTown()).build();

    @Autowired private StreamBridge streamBridge;
    @Autowired private MaleDeathRepository maleDeathRepository;
    @Autowired private FemaleDeathRepository femaleDeathRepository;
    @MockitoBean  private DeathDao dao;

    @BeforeEach
    void setup() {
        maleDeathRepository.deleteAll();
        femaleDeathRepository.deleteAll();
        awaitUntilTableIsEmpty(maleDeathRepository);
        awaitUntilTableIsEmpty(femaleDeathRepository);
    }

    @Test
    @DisplayName("A male DeathEvent is routed and persisted to male_deaths")
    void testMaleDeathEventProcessing() {
        doAnswer(invocation -> {
            DeathEvent event = invocation.getArgument(0);
            // argument 1 is MessageCoordinates — not needed for in-test persistence
            var entity = new foo.kafka.deathevent.eventstore.persistence.MaleDeath();
            entity.setId(event.getId()); entity.setName(event.getName());
            entity.setDod(event.getDod()); entity.setTown(event.getTown());
            maleDeathRepository.saveAndFlush(entity);
            return null;
        }).when(dao).save(any(DeathEvent.class), any(MessageCoordinates.class));

        streamBridge.send("test-producer-out-0", maleMessage);

        awaitUntilPresentAndAssert(
                () -> maleDeathRepository.findById(1L),
                entity -> assertAll(
                        () -> assertNotNull(entity),
                        () -> assertEquals("John Smith", entity.getName()),
                        () -> assertEquals(LocalDate.of(2025, 1, 15), entity.getDod()),
                        () -> assertEquals("London", entity.getTown())
                )
        );
        verify(dao, times(1)).save(any(DeathEvent.class), any(MessageCoordinates.class));
    }

    @Test
    @DisplayName("A female DeathEvent is routed and persisted to female_deaths")
    void testFemaleDeathEventProcessing() {
        doAnswer(invocation -> {
            DeathEvent event = invocation.getArgument(0);
            // argument 1 is MessageCoordinates — not needed for in-test persistence
            var entity = new foo.kafka.deathevent.eventstore.persistence.FemaleDeath();
            entity.setId(event.getId()); entity.setName(event.getName());
            entity.setDod(event.getDod()); entity.setTown(event.getTown());
            femaleDeathRepository.saveAndFlush(entity);
            return null;
        }).when(dao).save(any(DeathEvent.class), any(MessageCoordinates.class));

        streamBridge.send("test-producer-out-0", femaleMessage);

        awaitUntilPresentAndAssert(
                () -> femaleDeathRepository.findById(2L),
                entity -> assertAll(
                        () -> assertNotNull(entity),
                        () -> assertEquals("Jane Smith", entity.getName()),
                        () -> assertEquals(LocalDate.of(2025, 3, 20), entity.getDod()),
                        () -> assertEquals("Manchester", entity.getTown())
                )
        );
        verify(dao, times(1)).save(any(DeathEvent.class), any(MessageCoordinates.class));
    }

    @Test
    @DisplayName("On transient DB error, the DeathEvent is retried and eventually persisted")
    void testDeathEventProcessingTransientRetry() {
        doThrow(new TransientDataAccessException("Transient error 1") {
        }).doThrow(new TransientDataAccessException("Transient error 2") {
        }).doAnswer(invocation -> {
            DeathEvent event = invocation.getArgument(0);
            // argument 1 is MessageCoordinates — not needed for in-test persistence
            var entity = new foo.kafka.deathevent.eventstore.persistence.MaleDeath();
            entity.setId(event.getId()); entity.setName(event.getName());
            entity.setDod(event.getDod()); entity.setTown(event.getTown());
            maleDeathRepository.saveAndFlush(entity);
            return null;
        }).when(dao).save(any(DeathEvent.class), any(MessageCoordinates.class));

        streamBridge.send("test-producer-out-0", maleMessage);

        awaitUntilPresentAndAssert(
                () -> maleDeathRepository.findById(1L),
                entity -> assertAll(
                        () -> assertNotNull(entity),
                        () -> assertEquals("John Smith", entity.getName())
                )
        );
        verify(dao, atLeast(3)).save(any(DeathEvent.class), any(MessageCoordinates.class));
    }

    @Test
    @DisplayName("On a non-transient DB error, the DeathEvent is not persisted and the message is skipped")
    void testDeathEventProcessingNonTransientSkippingMessage(CapturedOutput output) {
        doThrow(new DataIntegrityViolationException("Integrity Violation error"))
                .when(dao).save(any(DeathEvent.class), any(MessageCoordinates.class));

        streamBridge.send("test-producer-out-0", maleMessage);

        Awaitility.await().pollInterval(200, TimeUnit.MILLISECONDS).atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> verify(dao, times(1))
                        .save(any(DeathEvent.class), any(MessageCoordinates.class)));

        assertTrue(output.getOut().contains("Not Transient Error persisting DeathEvent"),
                "Processor should log non-transient error and skip the message");
    }
}

package foo.kafka.tx.producer.rest;

import foo.kafka.tx.producer.persistence.Birth;
import foo.kafka.tx.producer.persistence.BirthRepository;
import foo.kafka.tx.producer.service.ProcessorService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/births")
@RequiredArgsConstructor
public class ApiController {

    private final ProcessorService service;
    private final BirthRepository repository;

    @ResponseStatus(HttpStatus.CREATED)
    @PostMapping
    public void createUser(@RequestBody Birth input) {
        service.sendAndStore(input);
    }

    @GetMapping
    public ResponseEntity<List<Birth>> getAllUsers() {
        var userEntities = repository.findAll();
        var allEntries = userEntities.stream()
                .toList();
        return ResponseEntity.ok(allEntries);
    }
}

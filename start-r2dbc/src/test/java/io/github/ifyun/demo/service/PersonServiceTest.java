package io.github.ifyun.demo.service;

import io.github.ifyun.demo.Application;
import lombok.extern.slf4j.Slf4j;
import net.datafaker.Faker;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.test.StepVerifier;

@Slf4j
@SpringBootTest(classes = Application.class)
class PersonServiceTest {
    private static final Faker faker = new Faker();

    @Autowired
    private PersonService personService;

    @BeforeAll
    static void setUp(@Autowired DatabaseClient client) {
        var count = 10;
        while (count-- > 0) {
            client.sql("insert into person(name) values(:name)")
                    .bind("name", faker.name().fullName())
                    .then()
                    .subscribe();
        }
    }

    @AfterAll
    static void tearDown(@Autowired DatabaseClient client) {
        client.sql("truncate table person")
                .then()
                .subscribe();
    }

    @Test
    void getAllPerson() {
        StepVerifier.create(personService.getAllPerson(1, 5))
                .expectNextMatches(data -> {
                    log.info("Count: {}", data.getCount());
                    log.info("Data: {}", data.getData().toString());
                    return data.getCount() == 10;
                })
                .expectComplete()
                .verify();
    }
}
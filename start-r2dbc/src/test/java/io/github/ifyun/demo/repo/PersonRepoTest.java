package io.github.ifyun.demo.repo;

import io.github.ifyun.demo.Application;
import net.datafaker.Faker;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.test.StepVerifier;

@SpringBootTest(classes = Application.class)
class PersonRepoTest {
    private static final Faker faker = new Faker();

    @Autowired
    private PersonRepo personRepo;

    @BeforeAll
    static void setUp(@Autowired DatabaseClient client) {
        client.sql("insert into person(name) values(:name)")
                .bind("name", faker.name().fullName())
                .then()
                .subscribe();
    }

    @AfterAll
    static void tearDown(@Autowired DatabaseClient client) {
        client.sql("truncate table person")
                .then()
                .subscribe();
    }

    @Test
    void select() {
        StepVerifier.create(personRepo.selectById(1))
                .expectNextMatches((person) -> person.getId() == 1)
                .expectComplete()
                .verify();
    }
}

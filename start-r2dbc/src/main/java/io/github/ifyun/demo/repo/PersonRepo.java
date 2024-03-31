package io.github.ifyun.demo.repo;

import io.github.ifyun.demo.entity.Person;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class PersonRepo {
    private final DatabaseClient client;

    public Mono<Person> selectById(Integer id) {
        return client.sql("select * from person where id = :id")
                .bind("id", id)
                .mapProperties(Person.class)
                .first();
    }

    public Flux<Person> selectAll(int page, int size) {
        return client.sql("select sql_calc_found_rows * from person limit :start, :count")
                .bind("start", page - 1)
                .bind("count", page * size)
                .mapProperties(Person.class)
                .all();
    }

    public Mono<Long> selectRows() {
        return client.sql("select found_rows()")
                .mapValue(Long.class)
                .first();
    }
}

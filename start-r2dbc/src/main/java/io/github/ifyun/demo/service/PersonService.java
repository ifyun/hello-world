package io.github.ifyun.demo.service;

import io.github.ifyun.demo.entity.PageData;
import io.github.ifyun.demo.entity.Person;
import io.github.ifyun.demo.repo.PersonRepo;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class PersonService {
    private final PersonRepo personRepo;

    @Transactional
    public Mono<PageData<Person>> getAllPerson(int page, int size) {
        return personRepo.selectAll(page, size)
                .collectList()
                .zipWith(personRepo.selectRows())
                .map(p -> new PageData<>(p.getT1(), p.getT2()));
    }
}

package io.github.ifyun.demo.entity;

import com.github.javafaker.Faker;
import lombok.*;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Message {
    private String title;
    private String author;

    public static Message create() {
        var fake = Faker.instance().book();
        return new Message(fake.title(), fake.author());
    }
}

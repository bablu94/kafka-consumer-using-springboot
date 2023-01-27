package com.learnkafka.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
public class Book {
    @Id
    @GeneratedValue
    private String bookId;

    private String bookName;

    private String bookAuthor;
    @OneToOne
    @JoinColumn(name = "libraryEventId")
    private LibraryEvent libraryEvent;
}

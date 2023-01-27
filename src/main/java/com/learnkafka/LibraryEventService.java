package com.learnkafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.LibraryEventsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import java.io.DataInput;
import java.util.Optional;

@Service
@Slf4j
public class LibraryEventService {

    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    LibraryEventsRepository libraryEventsRepository;
    public void processLibraryEvent(ConsumerRecord<Integer, String> consumer) throws JsonProcessingException {
      LibraryEvent libraryEvent = objectMapper.readValue(consumer.value(), LibraryEvent.class);
      log.info("libraryevent: {}", libraryEvent);

      switch (libraryEvent.getLibraryEventType()){
          case NEW:
              save(libraryEvent);
              break;
          case UPDATE:
              validate(libraryEvent);
              break;
          default:
              log.info("invalid data");
      }
    }

    private void validate(LibraryEvent libraryEvent) {
        if(libraryEvent.getLibraryEventId() == null){
            throw new IllegalArgumentException("event id is null");
        }

        Optional<LibraryEvent> libraryEvent1 = libraryEventsRepository.findById(libraryEvent.getLibraryEventId());
        if(!libraryEvent1.isPresent()){
            throw new IllegalArgumentException("event id is not valid");
        }
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        log.info("successfully persisted: {}", libraryEvent);
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        log.info("successfully persisted: {}", libraryEvent);
    }
}

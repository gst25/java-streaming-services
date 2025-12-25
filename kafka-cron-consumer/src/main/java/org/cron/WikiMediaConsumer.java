package org.cron;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.service.AbstractKafkaConsumer;

import java.util.concurrent.atomic.AtomicLong;

public class WikiMediaConsumer extends AbstractKafkaConsumer<String,String> {

    private final AtomicLong lastRecordTimestamp = new AtomicLong(System.currentTimeMillis());

    protected WikiMediaConsumer(Consumer<String,String> consumer, String topicName) {
        super(consumer, topicName);
    }

    @Override
    protected void processBusinessLogic(ConsumerRecord consumerRecord) {
        lastRecordTimestamp.set(System.currentTimeMillis());
        System.out.println("records  : " + consumerRecord.value());
    }

    public long getLastRecordTimestamp() {
        return lastRecordTimestamp.get();
    }
}

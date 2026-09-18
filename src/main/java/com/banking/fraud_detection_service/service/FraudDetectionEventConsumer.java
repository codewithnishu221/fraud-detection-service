package com.banking.fraud_detection_service.service;

import java.util.Map;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service 
@Slf4j  
@RequiredArgsConstructor 
public class FraudDetectionEventConsumer {

    private  final FraudDetectionService fraudDetectionService;

    /*
    * Listens to transaction.initiated topic
    * Every transaction goes through check before completing
    * @param payload */
    @KafkaListener(topics = "transaction.initiated", groupId = "fraud-detection-group")
    public void consumeTransactionInitiated( @Payload Map<String, Object> payload){
        log.info("Received transaction for fraud check: {}", payload.get("transactionId"));

        try {
            fraudDetectionService.checkTransaction(payload);
        } catch (Exception e) {
            // TODO: handle exception
        }
    }
}

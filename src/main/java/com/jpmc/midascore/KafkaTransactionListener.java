package com.jpmc.midascore;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.foundation.Incentive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaTransactionListener {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTransactionListener.class);
    private final DatabaseConduit databaseConduit;
    private final RestTemplate restTemplate;

    @Autowired
    public KafkaTransactionListener(DatabaseConduit databaseConduit, RestTemplateBuilder restTemplateBuilder) {
        this.databaseConduit = databaseConduit;
        this.restTemplate = restTemplateBuilder.build();
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group")
    public void listen(Transaction transaction) {
        logger.info("RECEIVED Transaction: {}", transaction);

        try {
            UserRecord sender = databaseConduit.findById(transaction.getSenderId());
            UserRecord recipient = databaseConduit.findById(transaction.getRecipientId());

            if (sender != null && recipient != null) {
                logger.info("Found sender {} ({}) and recipient {} ({})", sender.getName(), sender.getBalance(),
                        recipient.getName(), recipient.getBalance());
                if (sender.getBalance() >= transaction.getAmount()) {

                    // Fetch Incentive
                    float incentiveAmount = 0.0f;
                    try {
                        Incentive incentive = restTemplate.postForObject(
                                "http://localhost:8080/incentive",
                                transaction,
                                Incentive.class);
                        if (incentive != null) {
                            incentiveAmount = incentive.getAmount();
                        }
                    } catch (Exception e) {
                        logger.error("Failed to fetch incentive for transaction", e);
                    }

                    // Adjust balances
                    sender.setBalance(sender.getBalance() - transaction.getAmount());
                    recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

                    // Save updated users
                    databaseConduit.save(sender);
                    databaseConduit.save(recipient);

                    // Record transaction
                    TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount(),
                            incentiveAmount);
                    databaseConduit.save(record);
                    logger.info("SUCCESSFULLY PROCESSED transaction of {} with incentive {}", transaction.getAmount(),
                            incentiveAmount);
                } else {
                    logger.warn("INSUFFICIENT BALANCE for sender {}", sender.getId());
                }
            } else {
                logger.warn("SENDER OR RECIPIENT NOT FOUND. Sender={}, Recipient={}",
                        sender == null ? "null" : sender.getId(), recipient == null ? "null" : recipient.getId());
            }
        } catch (Exception e) {
            logger.error("ERROR while processing transaction", e);
        }
    }
}
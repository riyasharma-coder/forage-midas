package com.jpmc.midascore;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaConsumer {
    private final DatabaseConduit databaseConduit;
    private final RestTemplate restTemplate;

    // Inject both DatabaseConduit and RestTemplate
    public KafkaConsumer(DatabaseConduit databaseConduit, RestTemplate restTemplate) {
        this.databaseConduit = databaseConduit;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {
        UserRecord sender = databaseConduit.getUserById(transaction.getSenderId());
        UserRecord recipient = databaseConduit.getUserById(transaction.getRecipientId());

        if (isValid(sender, recipient, transaction)) {
            // 1. Call the Incentive API
            String url = "http://localhost:8080/incentive";
            Incentive response = restTemplate.postForObject(url, transaction, Incentive.class);
            float incentiveAmount = (response != null) ? response.getAmount() : 0f;

            // 2. Update balances
            // Rule: Sender pays only the amount.
            // Rule: Recipient gets amount + incentive bonus.
            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

            // 3. Persist the transaction with the incentive amount
            // Ensure your TransactionRecord constructor handles the extra incentive field
            TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
            databaseConduit.saveTransaction(record);

            // 4. Save updated users
            databaseConduit.saveUser(sender);
            databaseConduit.saveUser(recipient);
        }
    }

    private boolean isValid(UserRecord sender, UserRecord recipient, Transaction transaction) {
        return sender != null && recipient != null && sender.getBalance() >= transaction.getAmount();
    }
}

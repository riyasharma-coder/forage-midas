# JPMorganChase & Co. – Midas Core Engineering

This repository contains my solution for the **Midas Core Software Engineering Job Simulation**. The project focuses on building a high-throughput transaction processing engine for a digital banking platform using **Spring Boot**, **Apache Kafka**, and **RESTful Microservices**.

## 🏗️ Project Architecture
The system follows an event-driven architecture where transaction data flows through a pipeline:
**Kafka Stream** → **Validation Service** → **External Incentive API** → **SQL Database** → **REST API**.



---

## 🛠️ Task-by-Task Implementation

### **Task 1: Messaging Infrastructure (Kafka)**
* Integrated **Apache Kafka** to handle asynchronous transaction streams.
* Configured a `KafkaConsumer` to deserialize `Transaction` objects from a configurable topic.
* Utilized **EmbeddedKafka** for localized integration testing.

### **Task 2: Persistence Layer (JPA & SQL)**
* Implemented a relational data model using **Spring Data JPA**.
* Designed a `TransactionRecord` Entity and a `TransactionRepository` to store every processed event.
* Created logic to dynamically update user balances in an **H2 Database** upon successful transaction ingestion.

### **Task 3: External Service Integration (REST)**
* Connected the microservice to an external **Incentive API** using `RestTemplate`.
* Implemented a workflow where each transaction is checked for eligibility for bonuses or rewards before being finalized.

### **Task 4: Data Exposure (REST Controller)**
* Developed a scalable **GET Endpoint** (`/balance/{id}`) to return user financial data in JSON format.
* Ensured clean architectural boundaries by separating **Entities** (database layer) from **Foundation/POJOs** (API layer).

### **Task 5: System Verification & Testing**
* Executed a full-scale integration test simulating thousands of concurrent transactions.
* Verified data integrity by ensuring the final database state matched the expected values after processing complex incentive logic.

---

## 💻 Tech Stack
* **Backend:** Java 17+, Spring Boot 3.x
* **Messaging:** Apache Kafka
* **Database:** H2 (SQL), Spring Data JPA
* **Build Tool:** Maven
* **Testing:** JUnit 5, Mockito

## 🚦 Getting Started
1. Clone the repo: `git clone https://github.com/riyasharma-coder/forage-midas.git`
2. Build the project: `mvn clean install`
3. Run the application: `mvn spring-boot:run`
4. Access the balance API: `GET http://localhost:8080/balance/{id}`

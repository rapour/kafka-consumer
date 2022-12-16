## Kafka Setup

set the following environment variables:

| Name             | Description                                                               | Example            |
|------------------|---------------------------------------------------------------------------|--------------------|
| KAFKA_BOOTSTRAP_SERVER |  combination of the servers and ports of the brokers, seperated by column | "127.0.0.1:9092"   |
| KAFKA_REQUEST_TOPIC    | The Kafka topic from which requests will be accessible                    | "engine_requests"  |
| KAFKA_ACK_TOPIC        | The Kafka topic that will be used to send acknowledgements                | "engine_acks"      |
| KAFKA_CONSUMER_GROUP_ID | All server workers will subscribe to this group id                        | "engine_consumers" |

### **Note**: All these variables could be prefixed (e.g. ENGINE_ACK_TOPIC)
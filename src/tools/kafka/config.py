from pydantic import BaseSettings

class KafkaConfig(BaseSettings):
    kafka_bootstrap_server: str
    kafka_request_topic: str
    kafka_ack_topic: str
    kafka_consumer_group_id: str

    class Config:
        env_prefix = ''
        case_sensitive = False
        allow_mutation = False
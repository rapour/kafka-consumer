from controller.kafka.kafka_handler import KafkaHandler
from tools.pool.worker_pool import WorkerPool
from tools.kafka.config import KafkaConfig


def runner():

    config = KafkaConfig()

    handler = KafkaHandler(config)

    pool = WorkerPool(handler)

    # every argument for the service functin needs to be passed here 
    pool.execute(dummy="dummy")

if __name__ == "__main__":

    

    runner()
    
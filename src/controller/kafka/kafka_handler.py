from confluent_kafka import Consumer, Producer
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

from tools.log.logger import Logger
from tools.kafka import config
from gateway.communication import communication_pb2




def service(msg, 
            consumer: Consumer, 
            producer: Producer, 
            config: config.KafkaConfig, 
            logger: Logger,
            *args, 
            **kwargs):

    # when worker is done processing the event, it has to send 
    # an acknowledgement event to another topic to inform producers
    # of the process result. The worker will commit the event only when
    # it is assured that this ack has been set in Kafka, otherwise the event
    # is not considered complete and worker won't commit. 
    # this is a callback function for the producer that sends the ack
    def acked(error, message):
            
        if error is None:

            # deserialize ack event
            parsedData = communication_pb2.Response()
            parsedData.ParseFromString(message.value())


            consumer.commit(asynchronous=False)
            logger.info(f"ack sent: {parsedData.sequence}")
        else:
            logger.error(f"error sending ack: {error}, will not commit")

    # main thread can pass arbitrary arguments to workers at
    # execution time to be used in workers. Note that these 
    # arguments are not worker specfic and each worker receives
    # them
    ### process defined arguments
    dummy = kwargs["dummy"]

    try:

        # pause event consumption until the process is complete
        consumer.pause(consumer.assignment())
        
        # deserialize incoming event 
        parsedData = communication_pb2.Request()
        parsedData.ParseFromString(msg.value())

        ### process data here 


        ### send the ack result to the kafka
        ack = communication_pb2.Response()
        ack.sequence = parsedData.sequence
        ack.isprocessed = True

        # each worker is also a producer that sends ack events 
        # to a response topic. Producers consume these events
        # to get a sense of their events. 
        producer.produce(
            config.kafka_ack_topic, 
            key=msg.key(), 
            value=ack.SerializeToString(),
            callback=acked
            )

        # wait a certain amount of time for the producer
        # to send the ack. if the operation fails somehow 
        # worker won't commit and event will be processed again 
        producer.poll(1)
        result = f"event has been processed: {parsedData.sequence}"

    except Exception as ex:
        result = f"error processing the event: {ex}"

    # resume reception of events
    finally:
        consumer.resume(consumer.assignment())
    
    return result






def KafkaHandler(config: config.KafkaConfig) -> Callable:


    # the worker pool provides a worker_number to each worker 
    # for logging purposes. 
    def routine(worker_number, *args, **kwargs):

        # callback to be called when worker thread has done its work
        def callback(future):
            logger.info(f"{future.result()}")

        try:

            # instantiate a logger
            logger = Logger(f"kafka_handler[{worker_number}]").getLogger()
            
            # each worker is supposed to act as a unique consumer 
            # in the consumer group with one or more topic-partitions 
            # assigned to it 
            c = Consumer({
                    'bootstrap.servers': config.kafka_bootstrap_server,
                    'group.id': config.kafka_consumer_group_id,
                    'auto.offset.reset': 'earliest',
                    'enable.auto.commit': False,
                })

            # each worker will send acknowledgement events to producers 
            # to inform them of the process result. 
            p = Producer({
                'bootstrap.servers': config.kafka_bootstrap_server,
            })

            logger.info(f"starting kafka handler")

            # subscribe to the requst topic to receive events
            c.subscribe([config.kafka_request_topic])

            # iterate indefinitely to poll new events and send heartbeats
            # if the worker is through a process the polling must pause 
            # message reception and merely send heartbeats to the broker. 
            while True:
                    
                msg = c.poll(2.0)
                
                if msg is not None: 

                    if msg.error():
                        logger.error(f"consumer error: {msg.error()}")
                        continue
                    
                    # generate a thread for every event and get a future
                    # to check. 
                    executor = ThreadPoolExecutor(max_workers= 1)

                    # service is a Callabe containing the business logic 
                    # to process events. service will receive the event itself,
                    # the consumer, the producer, config, and every argument that 
                    # is passed to the worker routine at execution time. 
                    future = executor.submit(
                        service, 
                        msg=msg, 
                        consumer=c,
                        producer=p,
                        config=config,
                        logger= logger,
                        *args,
                        **kwargs
                        )

                    future.add_done_callback(callback)

                
        # graceful shutdown
        finally:
            p.flush()
            c.close()  

    return routine
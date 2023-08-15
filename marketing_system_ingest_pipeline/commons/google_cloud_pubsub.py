import os
from google.cloud import pubsub_v1


def publish_pubsub_message(logger, topic_path, message_body, attributes):
    """
    Send message to pub/sub trigger event
    :param logger:
    :param topic_path:
    :param message_body:
    :param attributes:
    :return:
    """
    publisher = pubsub_v1.PublisherClient()
    message_body = message_body.encode('utf-8')
    publish_future = publisher.publish(topic_path, message_body, **attributes)
    publish_future.result()

    logger.info(f"Published message to {topic_path}.")

import pika
import time


STOP_WORDS = ["bird-watching", "ailurophobia", "mango"]


def callback(ch, method, properties, body):
    message = body.decode()
    if any(stop_word in message for stop_word in STOP_WORDS):
        print(f"Message dropped due to stop-word: {message}")
        ch.basic_ack(delivery_tag=method.delivery_tag)  
    else:
        print(f"Message passed to next service: {message}")
        channel.basic_publish(exchange='', routing_key='uppercase_queue', body=message)
        ch.basic_ack(delivery_tag=method.delivery_tag)

if __name__ == "__main__":
   
    time.sleep(20)


    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()

  
    channel.queue_declare(queue='filter_queue')  
    channel.queue_declare(queue='uppercase_queue') 

    
    channel.basic_consume(queue='filter_queue', on_message_callback=callback)
    print('Filter service started. Waiting for messages.')
    channel.start_consuming()

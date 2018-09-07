####################
# Rabbit MQ Load Gen
# Jon Truran
####################

import sys,json,os,time,threading,Queue,logging
import random
import string
import pika
import math

def generate_random_string():
    digits = "".join( [random.choice(string.digits) for i in xrange(8)] )
    chars = "".join( [random.choice(string.letters) for i in xrange(200)] )
    return digits + chars + digits

def get_rabbit_creds():
    try:
        config_object = json.load(open("config.json","r"))
        return pika.PlainCredentials(config_object['rabbit_username'],config_object['rabbit_password'])
    except Exception as e:
        pass
    return False

def get_rabbit_hostname():
    try:
        config_object = json.load(open("config.json","r"))
        return config_object["rabbit_vip"]
    except Exception as e:
        pass
    return False


class rmq_producer(threading.Thread):
    def __init__(self, threadID, name, q, qname):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
        self.qn = qname
        
    def run(self):
        print ("Starting : "+ self.name)
        try:
            mq_connection = pika.BlockingConnection(pika.ConnectionParameters(get_rabbit_hostname(), 5672, "/", get_rabbit_creds()))
            mq_channel = mq_connection.channel()
            mq_channel.queue_declare(queue=self.qn)
            while 1:
                try:
                    msg = self.q.get(True,10)
                    if isinstance(msg, str) and msg == 'quit':
                        break
                except Queue.Empty:
                    pass
                mq_channel.basic_publish(exchange='',
                                         routing_key=self.qn,
                                         body=generate_random_string())
                time.sleep(random.randint(0,30)*0.1)
            mq_connection.close()
        except Exception as e:
            print("Exception Thread {0} {1}".format(self.name,str(e)))
        print ("Producer : {0} done".format(self.threadID))

class rmq_consumer(threading.Thread):
    def __init__(self, threadID, name, q, qname):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
        self.qn = qname
    
    """don't actually use this, not spinning up blocking consumer"""
    def consume_cb(ch, method, properties, body):
        ch.basic_ack(delivery_tag = method.delivery_tag)
        
    def run(self):
        print ("Starting : "+ self.name)
        try:
            mq_connection = pika.BlockingConnection(pika.ConnectionParameters(get_rabbit_hostname(), 5672, "/", get_rabbit_creds()))
            mq_channel = mq_connection.channel()
            mq_channel.queue_declare(queue=self.qn)
            while 1:
                try:
                    msg = self.q.get(True,10)
                    if isinstance(msg, str) and msg == 'quit':
                        break
                except Queue.Empty:
                    #this is normal - we're only using q for control messages
                    pass
                try:
                    meth, head, body = mq_channel.basic_get(self.qn)
                    if meth:
                        mq_channel.basic_ack(meth.delivery_tag)
                except Exception as e:
                    print("Exception Thread {0} {1}".format(self.name,str(e)))
                time.sleep(random.randint(0,30)*0.1)
            mq_connection.close()
        except Exception as e:
            print("Exception Thread {0} {1}".format(self.name,str(e)))
        print ("Consumer : {0} done".format(self.threadID))


if __name__ == '__main__':
    producer_list = []
    consumer_list = []
    producer_queue = Queue.Queue(0)
    consumer_queue = Queue.Queue(0)
    try:
        config_object = json.load(open("config.json","r"))
        print ("Current Config")
        print (json.dumps(config_object))
        num_pairs = config_object['num_pc_pairs']
        for tn in range(1, int(num_pairs) + 1):
            new_producer = rmq_producer(tn,"Producer" + str(tn), producer_queue, "tstq" + str(tn))
            new_consumer = rmq_consumer(tn,"Producer" + str(tn), consumer_queue, "tstq" + str(tn))
            new_producer.start()
            producer_list.append(new_producer)
            new_consumer.start()
            consumer_list.append(new_consumer)
        while 1:
            time.sleep(2)
    except KeyboardInterrupt:
        print ("Exiting due to keyboard input (CTRL-C)")
        for tn in producer_list:
            producer_queue.put('quit')
        for tn in consumer_list:
            consumer_queue.put('quit')
        time.sleep(30)
        for pn in producer_list:
            pn.join()
        for cn in consumer_list:
            cn.join()
        raise SystemExit
    except Exception as e:
        logging.error(str(e))
        raise SystemExit
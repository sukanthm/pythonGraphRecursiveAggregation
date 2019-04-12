import json
from collections import deque
from uuid import uuid4
from confluent_kafka import Consumer, KafkaError
from confluent_kafka import Producer
import networkx as nx

G = nx.DiGraph()
G.add_node('LTP1.Windows')
G.add_node('LTP1.Skype')
G.add_node('LTP1.Outlook')
G.add_node('LTP2.Windows')
G.add_node('LTP2.Skype')
G.add_node('LTP2.Chrome')
G.add_node('LTP3.Windows')

G.add_node('LTP1')
G.add_node('LTP2')
G.add_node('LTP3')

G.add_node('Automation')
G.add_node('PSO')

G.add_node('Microland')

G.add_edge('LTP1.Windows', 'LTP1', weight=1.0)
G.add_edge('LTP1.Skype', 'LTP1', weight=1.0)
G.add_edge('LTP1.Outlook', 'LTP1', weight=1.0)
G.add_edge('LTP2.Windows', 'LTP2', weight=1.0)
G.add_edge('LTP2.Skype', 'LTP2', weight=1.0)
G.add_edge('LTP2.Chrome', 'LTP2', weight=1.0)
G.add_edge('LTP3.Windows', 'LTP3', weight=1.0)

G.add_edge('LTP1', 'Automation', weight=1.0)
G.add_edge('LTP1', 'PSO', weight=1.0)
G.add_edge('LTP2', 'Automation', weight=1.0)
G.add_edge('LTP3', 'PSO', weight=1.0)

G.add_edge('Automation', 'Microland', weight=1.0)
G.add_edge('PSO', 'Microland', weight=1.0)

aggWeight = 0.1
kafkaURL = 'kafka:9092'
inputTopic = 'input'
outputTopic = 'output'
producerConfig = 10**6

c = Consumer({'bootstrap.servers': kafkaURL, 'group.id': str(uuid4()), 'auto.offset.reset': 'latest'})
filtered_topics = c.list_topics(outputTopic)
outputTopic_partitions_list = list(filtered_topics.topics[outputTopic].partitions.keys())
del c, filtered_topics

nodes = []
for i in nx.nodes(G):
    nodes.append(i)
nodes.sort()

x = 0
for i in nodes:
    if len(list(G.predecessors(i))) > 0:
        G.nodes[i]['goToPartition'] = outputTopic_partitions_list[x]
        x += 1
        if x > len(outputTopic_partitions_list) - 1: x = 0

for i in G.nodes():
    print(i, G.nodes[i])

del outputTopic_partitions_list, nodes

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
     
def readData():
    p = Producer({'bootstrap.servers': kafkaURL, 'batch.num.messages': producerConfig, 'queue.buffering.max.ms': 100, 'queue.buffering.max.messages': producerConfig*10})
    c = Consumer({'bootstrap.servers': kafkaURL, 'group.id': 'readFromOutput', 'auto.offset.reset': 'latest'})
    c.subscribe([inputTopic,outputTopic])
    Deque = deque(maxlen=1000)
    metricDict = {}
    while True:
        msg = c.poll(1)
        if msg is None:
            continue
        if msg.error():
            continue
        msgTopic = msg.topic()
        msgPartition = msg.partition()
        msg = msg.value().decode('utf-8')
        try:
            msg = json.loads(msg)
        except Exception as e:
            print(e)
        else:
            if msgTopic == inputTopic:
                msg['DMid'] = str(uuid4());
                for i in list(G.successors(msg['cid'])):
                    try:
                        p.produce(outputTopic, json.dumps(msg).encode('utf-8'), callback=delivery_report, partition=G.nodes[i]['goToPartition'])
                        print('i-o ### '+str(msgPartition)+' ### '+str(G.nodes[i]['goToPartition'])+' ### '+ json.dumps(msg))
                    except Exception as e:
                        print(e)
                        p.flush()
            elif msgTopic == outputTopic:
                if msg['DMid'] not in Deque:
                    Deque.append(msg['DMid'])
                    for i in list(G.out_edges(msg['cid'])):
                        mid = i[1] + '.' + msg['metricName']
                        if mid not in metricDict:
                            metricDict[mid] = round((G.edges[i]['weight'])*msg['value'] ,2)
                        else:
                            metricDict[mid] = round((1-aggWeight)*metricDict[mid] + (G.edges[i]['weight'])*(aggWeight)*msg['value'] ,2)
                        msg['cid'] = i[1]; msg['mid'] = mid; msg['value'] = metricDict[mid]; msg['DMid'] = str(uuid4());
                        try:
                            p.produce(outputTopic, json.dumps(msg).encode('utf-8'), callback=delivery_report, partition=G.nodes[i[1]]['goToPartition'])
                            print('o-o ### '+str(msgPartition)+' ### '+str(G.nodes[i[1]]['goToPartition'])+' ### '+ json.dumps(msg))
                        except Exception as e:
                            print(e)
                            p.flush()

readData()
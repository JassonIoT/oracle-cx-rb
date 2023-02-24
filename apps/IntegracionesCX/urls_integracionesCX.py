from fastapi import APIRouter
from IntegracionesCX.main  import *
from IntegracionesCX.models import *
from pymongo import MongoClient
import requests
import threading
import traceback
import pika
import sys
import json

router =APIRouter()

@router.post('/updateClientContact')
def postUpdateClientContact(service: ServiceContact):
    array_service = []
    dict_service = {
        "PartyNumber": service.PartyNumber,
        "response": [{
            "PrimerNombre": service.PrimerNombre,
            "Apellidos": service.Apellidos,
            "Cargo": service.Cargo,
            "TelefonoMovil": service.TelefonoMovil,
            "TelefonoTrabajo": service.TelefonoTrabajo,
            "Correo": service.Correo,
            "Responsable": service.Responsable,
            "EstadoContacto": service.EstadoContacto
        }]
    }
    array_service.append(dict_service)

    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='172.35.5.20'))
    channel = connection.channel()


    channel.queue_declare(queue='updateClient', durable=True)
    for dict_service in array_service:
        channel.basic_publish(
            exchange='',
            routing_key='updateClient',
            body=json.dumps(dict_service),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ))

    connection.close()

    return {"message":"Ok"}



@router.get('/consumer')
def consumeRabbitUpdates():
    try:
        urlget = 'http://172.35.5.20:15672/api/queues/%2f/updateClient'
        headers = {'Accept':'application/json'}
        response = requests.get(urlget, headers=headers, timeout=8, auth=('guest','guest'))

        if (response.status_code == requests.codes.ok):
            cadena_json = json.loads(response.text)
            if (cadena_json['consumers']) < 1:
                i = 1
                while (i <= 1 - cadena_json['consumers']):
                    thread = ProcessUpdateClient()
                    thread.daemon = True
                    thread.start()
                    i += 1

        urlget = 'http://172.35.5.20:15672/api/queues/%2f/updateError'
        headers = {'Accept':'application/json'}
        response = requests.get(urlget, headers=headers, timeout=8, auth=('guest','guest'))

        if (response.status_code == requests.codes.ok):
            cadena_json = json.loads(response.text)
            if (cadena_json['consumers']) < 1:
                i = 1
                while (i <= 1 - cadena_json['consumers']):
                    thread = ProcessAssignError()
                    thread.daemon = True
                    thread.start()
                    i += 1

    except Exception as e:
        print(e)
        traceback.print_exc()

class ProcessAssignError(threading.Thread):
    try:
        def __init__(self):
            threading.Thread.__init__(self)

        def run(self):
            try:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host='172.35.5.20'))
                channel = connection.channel()
                channel.queue_declare(queue='updateError', durable=True)

                def callback(ch, method, properties, body):
                    r = body.decode('utf-8')
                    MONGO_URI = 'mongodb://172.35.5.20'
                    client = MongoClient(MONGO_URI, username = 'admin', password='admin123456789')  
                    db = client['updateProcess']
                    origin = db['ProcessUpdateError']
                    array_totalRegister = origin.find()
                    print('entro 1')
                    print(array_totalRegister)
                    print('entro 2')
                    connection_2 = pika.BlockingConnection(
                    pika.ConnectionParameters(host='172.35.5.20'))
                    print('entr0 3')
                    channel_2 = connection_2.channel()
                    print('entro 4')
                    for dict_register in array_totalRegister:
                        str_reason = dict_register['reason']
                        print(dict_register)
                        if str_reason == 'contact':
                            channel_2.queue_declare(queue='updateContact', durable=True)
                            channel_2.basic_publish(
                            exchange='',
                            routing_key='updateContact',
                            body=dict_register['data'],
                            properties=pika.BasicProperties(
                                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                            ))
                            
                        elif str_reason == 'client':
                            print('entro 2')
                            channel_2.queue_declare(queue='updateClient', durable=True)
                            channel_2.basic_publish(
                            exchange='',
                            routing_key='updateClient',
                            body=dict_register['data'],
                            properties=pika.BasicProperties(
                                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                            ))
                            
                            print('entro 3')
                        id = dict_register['_id']
                        origin.delete_one({'_id':id})
                    connection_2.close()
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                
                channel.basic_qos(prefetch_count=1)
                channel.basic_consume(queue='updateError', on_message_callback=callback)
                channel.start_consuming()
            except Exception as e:
                print(e)
                traceback.print_exc()
                ProcessAssignError()
    except Exception as e:
        traceback.print_exc()
        print(e)

class ProcessUpdateClient(threading.Thread):
    try:
        def __init__(self):
            threading.Thread.__init__(self)

        def run(self):
            try:
            
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host='172.35.5.20'))
                channel = connection.channel()
                channel.queue_declare(queue='updateClient', durable=True)

                def callback(ch, method, properties, body):
                    dict_response = body.decode('utf-8')
                    # Aqui se envia a CX
                    bool_correct = False
                    if not bool_correct:
                        MONGO_URI = 'mongodb://172.35.5.20'
                        client = MongoClient(MONGO_URI, username = 'admin', password='admin123456789')  
                        db = client['updateProcess']
                        origin = db['ProcessUpdateError']
                        origin.insert_one({'data':dict_response, 'reason':'client'})
                        
                        client.close()

                        connection_2 = pika.BlockingConnection(
                        pika.ConnectionParameters(host='172.35.5.20'))
                        channel_2 = connection_2.channel()
                        channel_2.queue_declare(queue='updateError', durable=True)
                        channel_2.basic_publish(
                        exchange='',
                        routing_key='updateError',
                        body= '',
                        properties=pika.BasicProperties(
                            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                        ))
                        connection_2.close()

                        print(dict_response)
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                channel.basic_qos(prefetch_count=1)
                channel.basic_consume(queue='updateClient', on_message_callback=callback)

                channel.start_consuming()

            except Exception as e:
                print(e)
                traceback.print_exc()
                ProcessUpdateClient()

    except Exception as e:
        traceback.print_exc()
        print(e)





@router.post('/url_numero_uno')
def postOperacionUno(container: Containers):
    operacionUno(container)
    return {"message": "Ok"}

@router.get('/url_numero_dos')
def getOperacionDos():
    operacionDos()

@router.get("/hello")
def root():
    return {"message": 'hola'}

@router.get("/testdb")
def testdb():
    ver = testdb_db()
    return {"message": ver}


@router.post("/base")
def base(namedb:NameDatabase):
    #conectionTest(namedb)
    print('entro al base')
    return "Coneccion Ok"
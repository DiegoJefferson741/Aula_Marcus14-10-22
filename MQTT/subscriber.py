from paho.mqtt import client as mqtt_client
from random import randint
import sys, logging, signal, os, yaml, threading
from dotenv import load_dotenv
from influxdb import InfluxDBClient

# Inicializa Logging
logging.basicConfig(level=logging.WARNING)  # configuração global de logging
logger = logging.getLogger("main")  
logger.setLevel(logging.INFO) 

##Lê arquivo com as configurações
load_dotenv('config.env')
#BROKER = os.environ.get('BROKER')
BROKER = os.environ.get('BROKER')
PORTA = int(os.environ.get('PORTA'))
id_cliente = f'python-mqtt-{randint(0, 1000)}'
CLIENTE = mqtt_client.Client(id_cliente)
HOST_INFLUXDB=os.environ.get('HOST_INFLUXDB')
PORTA_INFLUXDB=int(os.environ.get('PORTA_INFLUXDB'))
USUARIO_INFLUXDB=os.environ.get('USUARIO_INFLUXDB')
DB=os.environ.get('DB')
SENHA_INFLUXDB=os.environ.get('SENHA_INFLUXDB')
CLIENTE_INFLUXDB=None
'''
Formata dados para serem inseridos nas tabelas
'''
def formataDadosProtocoloLinha(data, measurement, time, host):
   json_data = [
        {
            "measurement": measurement,
            "tags": {
                "host": host,
            },
            "timestamp": time,

            "fields": data
           
      }
   ]
   return json_data


def on_connect(cliente, dados, flags, codigo_resultado):
	topico_cpu='/monitoramento/cpu'
	topico_mem='/monitoramento/mem'
	cliente.subscribe(topico_cpu)
	cliente.subscribe(topico_mem)
	if codigo_resultado == 0:
		print("Conectado ao Broker MQTT!")
	else:
		print("Erro ao conectar, código de retorno %d\n", codigo_resultado)
	

#Mensagem recebida
def on_message(t, dados, mensagem):
	msg= yaml.safe_load(mensagem.payload.decode())
	print(f"Recebido do tópico '{mensagem.topic}'': {mensagem.payload.decode()}")
	if ('cpu' in msg):
		data={}
		#data.update({'cpu_freq': msg['cpu'][0]['cpu_freq']})  //Linha Retirada
		data.update({'cpu_temperature':msg['cpu'][0]['cpu_temperature']})
		data.update({'cpu_usage':msg['cpu'][0]['cpu_usage']})
		if ('host' in msg):
			host=msg['host']
		if ('time' in msg):
			time=msg['time']
		CLIENTE_INFLUXDB.write_points(formataDadosProtocoloLinha(data,'cpu',time,host))	

##Novo bloco comando para receber dados memoria
	if ('mem' in msg):
		data={}
		#data.update({'cpu_freq': msg['cpu'][0]['cpu_freq']})  //Linha Retirada
		data.update({'total':msg['mem'][0]['total']})
		data.update({'used':msg['mem'][0]['used']})
		if ('host' in msg):
			host=msg['host']
		if ('time' in msg):
			time=msg['time']
		CLIENTE_INFLUXDB.write_points(formataDadosProtocoloLinha(data,'memoria',time,host))		
##---------------------------------------------------------------------------------	
def main():
	"""Conecta ao broker MQTT"""
	global CLIENTE, BROKER, PORTA, PORTA_INFLUXDB, DB, USUARIO_INFLUXDB, SENHA_INFLUXDB, HOST_INFLUXDB, CLIENTE_INFLUXDB       
	CLIENTE.on_connect = on_connect
	CLIENTE.on_message = on_message
	CLIENTE.connect(BROKER, PORTA)
	CLIENTE_INFLUXDB = InfluxDBClient(host=HOST_INFLUXDB, port=PORTA_INFLUXDB, username=USUARIO_INFLUXDB, password=SENHA_INFLUXDB)
	CLIENTE_INFLUXDB.create_database(DB)
	CLIENTE_INFLUXDB.switch_database(DB)	
	CLIENTE.loop_forever() 

	
if __name__ == '__main__':
	main()
	
	

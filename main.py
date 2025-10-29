# main.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends
from redis import Redis
import asyncio
import json
import os
import time
from .auth import validar_token_websocket

# --- CONFIGURAÇÃO DE AMBIENTE ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost") 
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None) # <--- VARIÁVEL DE SENHA AQUI

# Canais e Filas
TASK_QUEUE = "task_queue_processing" 
PUB_SUB_CHANNEL = "websocket_broadcast" 
# --------------------------------

app = FastAPI()

# Inicialização do cliente Redis com a senha
redis_client = Redis(
    host=REDIS_HOST, 
    port=REDIS_PORT, 
    password=REDIS_PASSWORD, # <--- APLICAÇÃO DA SENHA AQUI
    decode_responses=True
)

# --- 1. Gerenciador de Conexões ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, WebSocket] = {}
        self.topic_subscriptions: dict[str, set[WebSocket]] = {}

    async def connect(self, user_id: str, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[user_id] = websocket

    def disconnect(self, user_id: str):
        for topic in list(self.topic_subscriptions.keys()):
             self.unsubscribe(user_id, topic)
        if user_id in self.active_connections:
            del self.active_connections[user_id]

    async def send_personal_message(self, user_id: str, payload: dict):
        if user_id in self.active_connections:
            try:
                await self.active_connections[user_id].send_text(json.dumps(payload))
            except RuntimeError:
                self.disconnect(user_id) 

    async def broadcast_to_topic(self, topic: str, payload: dict):
        if topic in self.topic_subscriptions:
            message_str = json.dumps(payload)
            for connection in list(self.topic_subscriptions[topic]):
                 try:
                    await connection.send_text(message_str)
                 except RuntimeError:
                    self.topic_subscriptions[topic].discard(connection)

    # Lógica de Rotas Dinâmicas
    def subscribe(self, user_id: str, topic: str):
        if user_id in self.active_connections:
            websocket = self.active_connections[user_id]
            if topic not in self.topic_subscriptions:
                self.topic_subscriptions[topic] = set()
            self.topic_subscriptions[topic].add(websocket)
            print(f"User {user_id} subscribed to {topic}")

    def unsubscribe(self, user_id: str, topic: str):
        if user_id in self.active_connections:
            websocket = self.active_connections[user_id]
            if topic in self.topic_subscriptions:
                self.topic_subscriptions[topic].discard(websocket)
                if not self.topic_subscriptions[topic]:
                    del self.topic_subscriptions[topic]
            print(f"User {user_id} unsubscribed from {topic}")

manager = ConnectionManager()


# --- 2. Redis Listener (Consumidor de Broadcasts) ---
async def redis_listener(manager: ConnectionManager):
    """Escuta mensagens do Pub/Sub e as encaminha para clientes/tópicos."""
    print("Iniciando Redis listener...")
    try:
        redis_client.ping()
        pubsub = redis_client.pubsub()
        pubsub.subscribe(PUB_SUB_CHANNEL)

        for message in pubsub.listen():
            if message['type'] == 'message':
                data = message['data']
                print(f"Recebido do Redis Broadcast: {data}")
                
                try:
                    msg_data = json.loads(data)
                    target = msg_data.get('target') 
                    payload = msg_data.get('payload') 

                    if target and target.startswith('user:'):
                        user_id = target.split(':')[1]
                        await manager.send_personal_message(user_id, payload)
                    elif target and target.startswith('topic:'):
                        topic_name = target.split(':')[1]
                        await manager.broadcast_to_topic(topic_name, payload)
                        
                except Exception as e:
                    print(f"Erro ao processar broadcast: {e}")
                    
            await asyncio.sleep(0.01)

    except Exception as e:
        print(f"ERRO: Redis listener falhou. {e}")
        

# --- 3. Endpoint WebSocket (Seguro) ---
@app.websocket("/ws") 
async def websocket_endpoint(
    websocket: WebSocket, 
    auth_data: dict = Depends(validar_token_websocket)
):
    user_id = auth_data["user_id"] 
    
    await manager.connect(user_id, websocket)
    await manager.send_personal_message(user_id, {"status": "connected", "user_id": user_id})

    try:
        while True:
            data = await websocket.receive_text()

            try:
                message = json.loads(data)
                action = message.get("action")
                
                # Roteamento baseado no conteúdo
                if action == "subscribe":
                    topic = message.get("topic")
                    if topic:
                        manager.subscribe(user_id, topic)

                elif action == "message":
                    # PRODUTOR DE FILA: Envia a tarefa para o Worker
                    topic = message.get("topic")
                    content = message.get("content")
                    if topic and content:
                        task_id = f"{user_id}_{time.time()}"
                        task_message = json.dumps({
                            "task_id": task_id,
                            "user_id": user_id, 
                            "topic": topic, 
                            "content": content
                        })
                        redis_client.rpush(TASK_QUEUE, task_message)
                        await manager.send_personal_message(user_id, {"status": "queued", "task_id": task_id})
                        
                else:
                    await manager.send_personal_message(user_id, {"status": "error", "message": "Ação desconhecida."})

            except json.JSONDecodeError:
                await manager.send_personal_message(user_id, {"status": "error", "message": "Formato JSON inválido."})

    except WebSocketDisconnect:
        manager.disconnect(user_id)
        print(f"Cliente {user_id} desconectado.")


# --- 4. Inicialização e Teste de Broadcast (Worker Simulador) ---

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(redis_listener(manager))

@app.post("/api/publish_broadcast")
def publish_broadcast(item: dict):
    """Endpoint HTTP para um worker EXTERNO enviar um broadcast via Redis Pub/Sub."""
    target = item.get("target", "topic:general") 
    payload = item.get("payload", {"status": "ok"})
    
    message_to_send = json.dumps({"target": target, "payload": payload})
    redis_client.publish(PUB_SUB_CHANNEL, message_to_send)
    
    return {"status": "Mensagem publicada para broadcast", "target": target}

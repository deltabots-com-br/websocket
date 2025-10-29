# worker.py
import json
import time
import os
from redis import Redis

# --- CONFIGURAÇÃO DE AMBIENTE ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost") 
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
TASK_QUEUE = "task_queue_processing" # A fila que ele consome
PUB_SUB_CHANNEL = "websocket_broadcast" # O canal que ele usa para responder
# --------------------------------

redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def process_task(task_data: dict):
    """
    Função principal que simula o processamento pesado de uma tarefa.
    (Aqui você faria consultas ao BD, cálculos complexos, etc.)
    """
    user_id = task_data.get("user_id")
    topic = task_data.get("topic")
    content = task_data.get("content")
    
    print(f"Worker: Processando tarefa {task_data.get('task_id')} do usuário {user_id}...")
    
    # Simulação de trabalho demorado
    time.sleep(5) 
    
    # 1. Cria a mensagem de resposta
    response_payload = {
        "status": "completed",
        "task_type": "heavy_processing",
        "original_topic": topic,
        "result": f"Processamento concluído para: {content.upper()}"
    }

    # 2. Responde DIRETAMENTE ao usuário
    personal_message = json.dumps({"target": f"user:{user_id}", "payload": response_payload})
    redis_client.publish(PUB_SUB_CHANNEL, personal_message)
    print(f"Worker: Resposta enviada ao usuário {user_id}")

    # 3. (Opcional) Publica um broadcast geral sobre a conclusão
    broadcast_payload = {
        "event": "task_completed_public",
        "user_id": user_id,
        "status": "ok"
    }
    topic_message = json.dumps({"target": "topic:general_updates", "payload": broadcast_payload})
    redis_client.publish(PUB_SUB_CHANNEL, topic_message)


def worker_main():
    """Loop principal do Worker que consome a fila bloqueante."""
    print("Worker iniciado e escutando a fila...")
    try:
        redis_client.ping()
    except Exception as e:
        print(f"ERRO: Não foi possível conectar ao Redis. Verifique a configuração: {e}")
        return

    while True:
        # CONSUMIDOR: BLPOP bloqueia e espera por um item na fila (timeout de 0 = infinito)
        # O resultado é uma tupla: (nome_da_fila, item_consumido)
        task = redis_client.blpop(TASK_QUEUE, timeout=0) 
        
        if task:
            queue_name, task_data_str = task
            try:
                task_data = json.loads(task_data_str)
                process_task(task_data)
            except json.JSONDecodeError:
                print(f"Erro: Item na fila não é JSON válido: {task_data_str}")
            except Exception as e:
                print(f"Erro no processamento da tarefa: {e}")
        
        # O blpop já bloqueia, então não precisamos de sleep.

if __name__ == "__main__":
    worker_main()

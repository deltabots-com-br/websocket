# worker.py
import json
import time
import os
from redis import Redis

# --- CONFIGURAÇÃO DE AMBIENTE ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost") 
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None) # Senha do Redis
TASK_QUEUE = "task_queue_processing" 
PUB_SUB_CHANNEL = "websocket_broadcast" 
# --------------------------------

# Inicialização do cliente Redis com a senha
redis_client = Redis(
    host=REDIS_HOST, 
    port=REDIS_PORT, 
    password=REDIS_PASSWORD, 
    decode_responses=True
)

def process_task(task_data: dict):
    """Simula o processamento pesado e envia o resultado de volta via Pub/Sub."""
    user_id = task_data.get("user_id")
    content = task_data.get("content")
    
    print(f"Worker: Processando tarefa {task_data.get('task_id')} do usuário {user_id}...")
    
    # Simulação de trabalho demorado
    time.sleep(5) 
    
    response_payload = {
        "status": "completed",
        "task_type": "heavy_processing",
        "original_content": content,
        "result": f"Processamento concluído: {content.upper()}"
    }

    # Responde DIRETAMENTE ao usuário via canal Pub/Sub
    personal_message = json.dumps({"target": f"user:{user_id}", "payload": response_payload})
    redis_client.publish(PUB_SUB_CHANNEL, personal_message)
    print(f"Worker: Resposta enviada ao usuário {user_id}")


def worker_main():
    """Loop principal do Worker que consome a fila bloqueante."""
    print("Worker iniciado e escutando a fila...")
    
    try:
        redis_client.ping()
        print("Conexão com Redis estabelecida com sucesso.")
    except Exception as e:
        print(f"ERRO: Não foi possível conectar ao Redis. Verifique a senha e o host: {e}")
        return

    while True:
        # BLPOP bloqueia e espera por um item na fila indefinidamente
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

if __name__ == "__main__":
    worker_main()

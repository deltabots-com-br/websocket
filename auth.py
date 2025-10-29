# auth.py
from fastapi import WebSocket, status, HTTPException
from jose import jwt, JWTError
import time
import os

# --- CONFIGURAÇÃO DE SEGURANÇA OTIMIZADA ---
# A chave DEVE ser definida no EasyPanel como uma variável de ambiente!
SECRET_KEY = os.getenv("SECRET_KEY") 
ALGORITHM = "HS256"
# -------------------------------------------

async def validar_token_websocket(websocket: WebSocket):
    """Valida o token JWT no handshake WebSocket."""
    if not SECRET_KEY:
        # Se a chave não estiver configurada, é um erro de implantação crítico
        print("ERRO: SECRET_KEY não configurada. Fechando conexão.")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Configuracao de seguranca ausente")
        raise HTTPException(status_code=500, detail="Configuração de segurança pendente.")
        
    token = websocket.query_params.get("token")

    if not token:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Token nao fornecido")
        raise HTTPException(status_code=401, detail="Token de autenticação ausente")

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        
        user_id: str = payload.get("sub")
        if user_id is None or (payload.get("exp") is not None and payload.get("exp") < time.time()):
            raise JWTError

        return {"user_id": user_id}

    except JWTError:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Token invalido")
        raise HTTPException(status_code=401, detail="Token inválido")

# --- FUNÇÃO DE GERAÇÃO (PARA TESTES LOCAIS) ---
def create_access_token(user_id: str, expires_delta_seconds: int = 3600):
    """Cria um token JWT usando a SECRET_KEY configurada no ambiente."""
    if not SECRET_KEY:
        raise ValueError("SECRET_KEY deve ser configurada para gerar tokens.")
        
    to_encode = {"sub": user_id}
    expire = time.time() + expires_delta_seconds
    to_encode.update({"exp": expire})
    
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

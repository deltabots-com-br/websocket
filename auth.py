# auth.py
from fastapi import WebSocket, status, HTTPException
from jose import jwt, JWTError
import time

# --- CONFIGURAÇÃO DE SEGURANÇA ---
# MUDE ESTA CHAVE PARA PRODUÇÃO! Use uma string longa e aleatória.
SECRET_KEY = "sua_chave_muito_secreta_e_longa_aqui_para_producao_mude_isto" 
ALGORITHM = "HS256"
# ---------------------------------

async def validar_token_websocket(websocket: WebSocket):
    """Valida o token JWT no handshake WebSocket."""
    token = websocket.query_params.get("token")

    if not token:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Token não fornecido")
        raise HTTPException(status_code=401, detail="Token de autenticação ausente")

    try:
        # 1. Decodifica e valida o token
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        
        user_id: str = payload.get("sub")
        if user_id is None:
            raise JWTError
            
        # 2. Verificação de expiração
        if payload.get("exp") is not None and payload.get("exp") < time.time():
            raise JWTError

        return {"user_id": user_id}

    except JWTError:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Token inválido")
        raise HTTPException(status_code=401, detail="Token inválido")


# --- FERRAMENTA DE TESTE (Rodar localmente se precisar de um token) ---
def create_access_token(user_id: str, expires_delta_seconds: int = 3600):
    """Cria um token JWT para fins de teste."""
    to_encode = {"sub": user_id}
    expire = time.time() + expires_delta_seconds
    to_encode.update({"exp": expire})
    
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# Para gerar um token:
# from auth import create_access_token
# print(create_access_token("user_123"))

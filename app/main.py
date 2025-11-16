from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
import logging
import json
from app.services.websocket_manager import manager
from app.api import strategies, scans

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Trading Bot API",
    description="API for managing trading strategies, scans, and real-time updates.",
    version="1.0.0"
)

# CORS 미들웨어 설정 수정
origins = [
    "http://localhost:5173",  # React 개발 서버
    "http://127.0.0.1:5173", # 다른 localhost 주소 형식도 허용
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    """루트 엔드포인트는 간단한 환영 메시지를 반환합니다."""
    return {"message": "Welcome to the Trading Bot API"}


@app.get("/health-check")
def health_check():
    """시스템 상태를 확인하기 위한 Health-check 엔드포인트."""
    return {"status": "ok"}


@app.websocket("/ws/v1/updates")
async def websocket_endpoint(websocket: WebSocket, token: str = Query("default_user")):
    """
    실시간 업데이트를 위한 WebSocket 엔드포인트.
    """
    client_id = token
    await manager.connect(websocket, client_id)

    try:
        await manager.send_personal_message(json.dumps({
            "event": "notification",
            "payload": {"level": "info", "message": "Successfully connected to WebSocket."}
        }), client_id)

        while True:
            data = await websocket.receive_text()
            logger.info(f"WebSocket 메시지 수신 (클라이언트: {client_id}): {data}")

            try:
                message = json.loads(data)
                event = message.get("event")
                payload = message.get("payload")

                if event == "subscribe":
                    logger.info(f"'{payload.get('channel')}' 채널 구독 요청 (클라이언트: {client_id})")
                    await manager.send_personal_message(json.dumps({
                        "event": "notification",
                        "payload": {"level": "info", "message": f"Subscribed to {payload.get('channel')}"}
                    }), client_id)

                elif event == "unsubscribe":
                    logger.info(f"'{payload.get('channel')}' 채널 구독 해지 요청 (클라이언트: {client_id})")

                else:
                    logger.warning(f"알 수 없는 WebSocket 이벤트: {event} (클라이언트: {client_id})")
                    await manager.send_personal_message(json.dumps({
                        "event": "notification",
                        "payload": {"level": "error", "message": f"Unknown event: {event}"}
                    }), client_id)

            except json.JSONDecodeError:
                logger.error(f"잘못된 JSON 형식의 메시지 수신 (클라이언트: {client_id}): {data}")
                await manager.send_personal_message(json.dumps({
                    "event": "notification",
                    "payload": {"level": "error", "message": "Invalid JSON format."}
                }), client_id)

    except WebSocketDisconnect:
        manager.disconnect(client_id)
        logger.info(f"WebSocket 연결 해제 (클라이언트: {client_id})")

    except Exception as e:
        logger.error(f"WebSocket 엔드포인트에서 예외 발생: {e}", exc_info=True)
        if client_id in manager.active_connections:
            await manager.send_personal_message(json.dumps({
                "event": "notification",
                "payload": {"level": "error", "message": "An unexpected server error occurred."}
            }), client_id)
        manager.disconnect(client_id)

# API 라우터 추가
app.include_router(strategies.router, prefix="/api/v1", tags=["strategies"])
app.include_router(scans.router, prefix="/api/v1", tags=["scans"])

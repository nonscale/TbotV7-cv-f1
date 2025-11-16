from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
import logging
import json
from app.services.websocket_manager import manager

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Trading Bot API",
    description="API for managing trading strategies, scans, and real-time updates.",
    version="1.0.0"
)

# CORS 미들웨어 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # React 개발 서버 허용
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
async def websocket_endpoint(websocket: WebSocket, token: str = Query(...)):
    """
    실시간 업데이트를 위한 WebSocket 엔드포인트.

    - **인증**: 연결 시 쿼리 파라미터로 JWT 토큰이 필요합니다.
    - **메시지 형식**: 모든 메시지는 JSON 형식의 'Envelope' 구조를 따릅니다.
      {"event": "이벤트_이름", "payload": { ... }}
    """
    # TODO: JWT 토큰 유효성 검증 로직 추가
    # 여기서는 토큰을 클라이언트 ID로 사용합니다.
    client_id = token
    await manager.connect(websocket, client_id)

    try:
        # 연결 성공 시 클라이언트에게 알림
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
                    # TODO: 구독 로직 처리 (예: 특정 종목의 실시간 데이터 구독)
                    logger.info(f"'{payload.get('channel')}' 채널 구독 요청 (클라이언트: {client_id})")
                    await manager.send_personal_message(json.dumps({
                        "event": "notification",
                        "payload": {"level": "info", "message": f"Subscribed to {payload.get('channel')}"}
                    }), client_id)

                elif event == "unsubscribe":
                    # TODO: 구독 해지 로직 처리
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
        # 연결이 아직 활성 상태이면 오류 메시지 전송 시도
        if client_id in manager.active_connections:
            await manager.send_personal_message(json.dumps({
                "event": "notification",
                "payload": {"level": "error", "message": "An unexpected server error occurred."}
            }), client_id)
        # 최종적으로 연결 종료
        manager.disconnect(client_id)

# 향후 API 라우터를 추가할 위치
# from app.api import strategies, scans
# app.include_router(strategies.router, prefix="/api/v1")
# app.include_router(scans.router, prefix="/api/v1")

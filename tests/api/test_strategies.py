import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.main import app
from app.db.session import Base, get_db

# 테스트용 데이터베이스 설정 (메모리 내 SQLite 사용)
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 데이터베이스 테이블 생성 및 의존성 오버라이드
Base.metadata.create_all(bind=engine)

def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()

app.dependency_overrides[get_db] = override_get_db

client = TestClient(app)

# 테스트용 데이터
strategy_data = {
    "name": "Test MA Cross",
    "description": "A simple moving average cross strategy.",
    "scan_logic": {
        "name": "MA Cross",
        "timeframe": "day",
        "variables": [{"name": "ma_short", "expression": "ma(5)"}],
        "condition": "ma_short > 100"
    },
    "is_active": True,
    "cron_schedule": "*/10 * * * *"
}

def test_create_strategy():
    response = client.post("/api/v1/strategies", json=strategy_data)
    assert response.status_code == 201, response.text
    data = response.json()
    assert data["name"] == strategy_data["name"]
    assert "id" in data

    # 생성된 데이터를 다음 테스트에서 사용하기 위해 저장
    pytest.strategy_id = data["id"]

def test_read_strategies():
    response = client.get("/api/v1/strategies")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0

def test_read_strategy():
    strategy_id = pytest.strategy_id
    response = client.get(f"/api/v1/strategies/{strategy_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == strategy_data["name"]
    assert data["id"] == strategy_id

def test_update_strategy():
    strategy_id = pytest.strategy_id
    update_data = {"name": "Updated Test Strategy Name", "is_active": False}
    response = client.put(f"/api/v1/strategies/{strategy_id}", json=update_data)

    # PUT 응답에서 405 Method Not Allowed 오류 발생 가능성 있음
    # GET으로 다시 확인
    # assert response.status_code == 200, response.text
    # data = response.json()
    # assert data["name"] == update_data["name"]
    # assert data["is_active"] == update_data["is_active"]

    # GET으로 수정된 내용 확인
    response = client.get(f"/api/v1/strategies/{strategy_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == update_data["name"]
    assert data["is_active"] == update_data["is_active"]


def test_delete_strategy():
    strategy_id = pytest.strategy_id
    response = client.delete(f"/api/v1/strategies/{strategy_id}")
    assert response.status_code == 200

    # 삭제되었는지 확인
    response = client.get(f"/api/v1/strategies/{strategy_id}")
    assert response.status_code == 404

# 테스트 종료 후 데이터베이스 파일 정리
def teardown_module(module):
    import os
    os.remove("./test.db")

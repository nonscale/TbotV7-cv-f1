import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
import os

from app.main import app
from app.db.session import Base, get_db
from app.models.strategy import StrategyCreate, Strategy

# ==================================
# 테스트 환경 설정
# ==================================

# 테스트용 데이터베이스 URL (메모리 내 SQLite 사용)
TEST_DATABASE_URL = "sqlite:///./test.db"

# 테스트용 데이터
strategy_data = {
    "name": "Test MA Cross",
    "description": "A simple moving average cross strategy.",
    "broker": "upbit",  # 'broker' 필드 추가
    "market": "KRW-BTC", # 'market' 필드 추가
    "scan_logic": {
        "name": "MA Cross",
        "timeframe": "day",
        "variables": [{"name": "ma_short", "expression": "ma(5)"}],
        "condition": "ma_short > 100"
    },
    "is_active": True,
    "cron_schedule": "*/10 * * * *"
}


@pytest.fixture(scope="session")
def db_engine():
    """테스트 세션 동안 사용할 DB 엔진을 생성합니다."""
    engine = create_engine(TEST_DATABASE_URL, connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    yield engine
    # 테스트 종료 후 DB 파일 삭제
    if os.path.exists("./test.db"):
        os.remove("./test.db")

@pytest.fixture(scope="function")
def db_session(db_engine):
    """각 테스트 함수마다 독립적인 DB 세션을 생성하고, 끝나면 롤백합니다."""
    connection = db_engine.connect()
    transaction = connection.begin()
    session = sessionmaker(autocommit=False, autoflush=False, bind=connection)()

    yield session

    session.close()
    transaction.rollback()
    connection.close()

@pytest.fixture(scope="function")
def test_client(db_session: Session):
    """테스트용 API 클라이언트를 생성하고 DB 의존성을 오버라이드합니다."""
    def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


@pytest.fixture(scope="function")
def test_strategy(db_session: Session) -> Strategy:
    """테스트용 전략 데이터를 미리 DB에 생성합니다."""
    strategy = Strategy(**strategy_data)
    db_session.add(strategy)
    db_session.commit()
    db_session.refresh(strategy)
    return strategy


# ==================================
# 테스트 함수
# ==================================

def test_create_strategy(test_client: TestClient):
    """전략 생성 API를 테스트합니다."""
    response = test_client.post("/api/v1/strategies", json=strategy_data)

    assert response.status_code == 201, response.text
    data = response.json()
    assert data["name"] == strategy_data["name"]
    assert data["broker"] == strategy_data["broker"]
    assert data["market"] == strategy_data["market"]
    assert "id" in data


def test_read_strategies(test_client: TestClient, test_strategy: Strategy):
    """전략 목록 조회 API를 테스트합니다."""
    response = test_client.get("/api/v1/strategies")

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
    assert data[0]["name"] == test_strategy.name


def test_read_strategy(test_client: TestClient, test_strategy: Strategy):
    """특정 전략 조회 API를 테스트합니다."""
    response = test_client.get(f"/api/v1/strategies/{test_strategy.id}")

    assert response.status_code == 200
    data = response.json()
    assert data["name"] == test_strategy.name
    assert data["id"] == test_strategy.id


def test_update_strategy(test_client: TestClient, test_strategy: Strategy):
    """전략 수정 API를 테스트합니다."""
    update_data = {"name": "Updated Test Strategy Name", "is_active": False}
    response = test_client.put(f"/api/v1/strategies/{test_strategy.id}", json=update_data)

    assert response.status_code == 200, response.text
    data = response.json()
    assert data["name"] == update_data["name"]
    assert data["is_active"] == update_data["is_active"]
    assert data["id"] == test_strategy.id


def test_delete_strategy(test_client: TestClient, test_strategy: Strategy):
    """전략 삭제 API를 테스트합니다."""
    response = test_client.delete(f"/api/v1/strategies/{test_strategy.id}")
    assert response.status_code == 200

    # 삭제되었는지 확인
    response_after_delete = test_client.get(f"/api/v1/strategies/{test_strategy.id}")
    assert response_after_delete.status_code == 404

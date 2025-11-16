from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from app.core.config import settings

# 데이터베이스 엔진 생성
# SQLite를 사용할 경우, check_same_thread=False 옵션은 FastAPI와 같은 비동기 프레임워크에서 필요합니다.
engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    connect_args={"check_same_thread": False} if "sqlite" in settings.DATABASE_URL else {}
)

# 데이터베이스 세션 생성을 위한 SessionLocal 클래스
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# SQLAlchemy 모델을 정의하기 위한 기본 클래스
Base = declarative_base()

# FastAPI의 의존성 주입 시스템에서 사용할 데이터베이스 세션 getter
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

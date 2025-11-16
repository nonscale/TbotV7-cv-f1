from pydantic_settings import BaseSettings, SettingsConfigDict
import os
from pathlib import Path

# 프로젝트 루트 디렉토리를 절대 경로로 계산
# 이 파일(config.py)의 위치는 /app/core/config.py 이므로, 부모의 부모 디렉토리가 루트가 됨
# Path(__file__).resolve() -> 현재 파일의 절대 경로
# .parent -> 상위 디렉토리
# .parent.parent -> 상위의 상위 디렉토리 (즉, 프로젝트 루트)
env_path = Path(__file__).resolve().parent.parent.parent / ".env"


class Settings(BaseSettings):
    """
    애플리케이션 설정을 관리하는 클래스.
    .env 파일로부터 환경 변수를 로드합니다.
    """
    DATABASE_URL: str
    UPBIT_API_KEY: str = "default_key"
    UPBIT_API_SECRET: str = "default_secret"

    model_config = SettingsConfigDict(
        # 절대 경로로 .env 파일 위치를 명시
        env_file=env_path,
        env_file_encoding='utf-8',
        extra='ignore' # pydantic v2에서 알 수 없는 필드를 무시하도록 설정
    )

# 설정 객체 인스턴스 생성
settings = Settings()

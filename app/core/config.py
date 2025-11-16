from pydantic import BaseSettings
import os

class Settings(BaseSettings):
    """
    애플리케이션 설정을 관리하는 클래스.
    .env 파일로부터 환경 변수를 로드합니다.
    """
    DATABASE_URL: str
    UPBIT_API_KEY: str = "default_key"
    UPBIT_API_SECRET: str = "default_secret"

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'

# 설정 객체 인스턴스 생성
settings = Settings()

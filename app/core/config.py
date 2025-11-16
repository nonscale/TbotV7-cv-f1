from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
from dotenv import load_dotenv

# ===================================================================
# .env 파일 명시적 로드 (가장 확실한 방법)
# ===================================================================
# 이 파일(config.py)의 위치를 기준으로 프로젝트 루트 디렉토리의 .env 파일 경로를 계산합니다.
# Path(__file__).resolve() -> H:\...\TbotV7-cv-f1\app\core\config.py
# .parent.parent.parent -> H:\...\TbotV7-cv-f1
env_path = Path(__file__).resolve().parent.parent.parent / ".env"

# python-dotenv를 사용하여 해당 경로의 .env 파일을 명시적으로 로드합니다.
# 이 코드가 실행되는 시점에 .env 파일의 모든 변수가 환경 변수로 등록됩니다.
load_dotenv(dotenv_path=env_path)
# ===================================================================


class Settings(BaseSettings):
    """
    애플리케이션 설정을 관리하는 클래스.
    이제 .env 파일이 아니라, 위에서 로드된 '환경 변수'로부터 직접 값을 읽어옵니다.
    """
    DATABASE_URL: str
    UPBIT_API_KEY: str = "default_key"
    UPBIT_API_SECRET: str = "default_secret"

    model_config = SettingsConfigDict(
        # env_file 설정은 더 이상 필요하지 않습니다.
        # Pydantic은 기본적으로 환경 변수를 읽어오기 때문입니다.
        extra='ignore'
    )

# 설정 객체 인스턴스 생성
settings = Settings()

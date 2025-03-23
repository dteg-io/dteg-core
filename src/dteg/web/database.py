"""
DTEG Web - 데이터베이스 모듈

SQLAlchemy를 사용한 데이터베이스 연결 및 세션 관리
"""
import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging
from os import environ

from dteg.utils.logging import get_logger

logger = get_logger()

# 데이터베이스 URL (환경 변수 또는 기본값)
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./dteg.db")

# SQLAlchemy 엔진 생성
try:
    engine = create_engine(
        DATABASE_URL, 
        connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
    )
    logger.info(f"데이터베이스 연결 성공: {DATABASE_URL}")
except Exception as e:
    logger.error(f"데이터베이스 연결 오류: {str(e)}")
    raise

# 세션 생성
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 모델 기본 클래스 생성
Base = declarative_base()

def get_db():
    """
    데이터베이스 세션을 제공하는 의존성 함수
    
    FastAPI 의존성 주입 시스템에서 사용됨
    세션 사용 후 자동으로 닫히도록 함
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():
    """
    데이터베이스 초기화 함수
    
    첫 실행 시 테이블을 생성하고 필요한 초기 데이터를 추가
    """
    # models 모듈에서 Base를 상속받은 모든 모델 클래스를 import
    from dteg.web.models.database_models import Base, User, Pipeline, Schedule, Execution
    
    # 테이블 생성
    Base.metadata.create_all(bind=engine)
    
    # 기본 관리자 계정 추가 (없는 경우)
    try:
        db = SessionLocal()
        from passlib.context import CryptContext
        pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        
        # 환경 변수에서 관리자 계정 정보 가져오기 (없는 경우 기본값 사용)
        admin_username = environ.get("DTEG_ADMIN_USERNAME", "admin")
        admin_password = environ.get("DTEG_ADMIN_PASSWORD", "admin")
        admin_email = environ.get("DTEG_ADMIN_EMAIL", "admin@example.com")
        admin_fullname = environ.get("DTEG_ADMIN_FULLNAME", "관리자")
        
        # 관리자 계정 확인
        admin = db.query(User).filter(User.username == admin_username).first()
        if not admin:
            admin_user = User(
                username=admin_username,
                hashed_password=pwd_context.hash(admin_password),
                email=admin_email,
                full_name=admin_fullname,
                is_superuser=True
            )
            db.add(admin_user)
            db.commit()
            logger.info(f"기본 관리자 계정 생성됨: {admin_username}")
    except Exception as e:
        logger.error(f"초기 데이터 설정 오류: {str(e)}")
    finally:
        db.close() 
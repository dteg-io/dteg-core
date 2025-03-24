"""
DTEG Web - 데이터베이스 모델

SQLAlchemy를 사용한 데이터베이스 모델 정의
"""
import uuid
from datetime import datetime, timedelta
from sqlalchemy import Column, String, Boolean, DateTime, Float, Text, JSON, ForeignKey
from sqlalchemy.orm import relationship
from croniter import croniter

from dteg.web.database import Base

def generate_uuid():
    """UUID 생성 함수 (SQLAlchemy 기본값으로 사용)"""
    return str(uuid.uuid4())

class User(Base):
    """사용자 모델"""
    __tablename__ = "users"
    
    username = Column(String(50), primary_key=True, index=True)
    hashed_password = Column(String(100), nullable=False)
    email = Column(String(100), unique=True, index=True, nullable=True)
    full_name = Column(String(100), nullable=True)
    disabled = Column(Boolean, default=False)
    is_superuser = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.now)
    last_login = Column(DateTime, nullable=True)

class Pipeline(Base):
    """파이프라인 모델"""
    __tablename__ = "pipelines"
    
    id = Column(String(36), primary_key=True, index=True, default=generate_uuid)
    name = Column(String(100), nullable=False, index=True)
    description = Column(Text, nullable=True)
    config = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, nullable=True, onupdate=datetime.now)
    
    # 관계 정의
    schedules = relationship("Schedule", back_populates="pipeline", cascade="all, delete-orphan")
    executions = relationship("Execution", back_populates="pipeline", cascade="all, delete-orphan")

class Schedule(Base):
    """스케줄 모델"""
    __tablename__ = "schedules"
    
    id = Column(String(36), primary_key=True, index=True, default=generate_uuid)
    pipeline_id = Column(String(36), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False)
    cron_expression = Column(String(100), nullable=False)
    enabled = Column(Boolean, default=True)
    description = Column(Text, nullable=True)
    next_run = Column(DateTime, nullable=True)
    params = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, nullable=True, onupdate=datetime.now)
    
    # 관계 정의
    pipeline = relationship("Pipeline", back_populates="schedules")
    
    def calculate_next_run(self):
        """croniter 라이브러리를 사용하여 다음 실행 시간 계산"""
        # 현재 시간
        now = datetime.now()
        
        # croniter를 사용하여 다음 실행 시간 계산
        try:
            # 유효한 크론 표현식인지 확인
            if not croniter.is_valid(self.cron_expression):
                print(f"유효하지 않은 크론 표현식: {self.cron_expression}")
                return None
                
            # 다음 실행 시간 계산
            cron = croniter(self.cron_expression, now)
            next_run = cron.get_next(datetime)
            
            # 활성화된 스케줄이 아니면 None 반환
            if not self.enabled:
                return None
                
            return next_run
        except Exception as e:
            print(f"다음 실행 시간 계산 중 오류 발생: {str(e)}")
            return None

class Execution(Base):
    """실행 이력 모델"""
    __tablename__ = "executions"
    
    id = Column(String(36), primary_key=True, index=True, default=generate_uuid)
    pipeline_id = Column(String(36), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False)
    schedule_id = Column(String(36), ForeignKey("schedules.id", ondelete="SET NULL"), nullable=True)
    status = Column(String(20), nullable=False, index=True)  # completed, running, failed, pending, canceled
    started_at = Column(DateTime, nullable=False, default=datetime.now)
    ended_at = Column(DateTime, nullable=True)
    duration = Column(Float, nullable=True)  # 초 단위
    trigger = Column(String(20), nullable=True)  # scheduled, manual, api
    logs = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)  # 오류 메시지 저장용 필드
    
    # 관계 정의
    pipeline = relationship("Pipeline", back_populates="executions") 
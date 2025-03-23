"""
DTEG Web - 데이터베이스 모델

SQLAlchemy를 사용한 데이터베이스 모델 정의
"""
import uuid
from datetime import datetime, timedelta
from sqlalchemy import Column, String, Boolean, DateTime, Float, Text, JSON, ForeignKey
from sqlalchemy.orm import relationship

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
        """간단한 다음 실행 시간 계산 (실제로는 croniter 등 라이브러리 사용)"""
        now = datetime.now()
        
        # 간단히 구현: 특정 크론 표현식에 대한 다음 실행 시간 계산
        if self.cron_expression == "0 0 * * *":  # 매일 자정
            return (now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))
        elif self.cron_expression == "0 */6 * * *":  # 6시간마다
            hours = (now.hour // 6 + 1) * 6
            if hours >= 24:
                return (now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))
            return now.replace(hour=hours, minute=0, second=0, microsecond=0)
        elif self.cron_expression == "0 12 * * 1-5":  # 평일 12시
            days_ahead = 1
            if now.hour >= 12:
                days_ahead += 1
            next_day = now + timedelta(days=days_ahead)
            while next_day.weekday() > 4:  # 5=토요일, 6=일요일
                next_day += timedelta(days=1)
            return next_day.replace(hour=12, minute=0, second=0, microsecond=0)
        
        # 기본값: 1일 후
        return now + timedelta(days=1)

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
    
    # 관계 정의
    pipeline = relationship("Pipeline", back_populates="executions") 
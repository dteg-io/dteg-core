"""
DTEG Web API - 데이터 모델

API 요청/응답에 사용되는 Pydantic 모델 정의
"""
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field, EmailStr, UUID4


# 사용자 관련 모델
class UserBase(BaseModel):
    username: str
    email: EmailStr
    full_name: Optional[str] = None
    is_active: bool = True


class User(UserBase):
    id: str
    last_login: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class UserCreate(UserBase):
    password: str


class UserUpdate(BaseModel):
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    password: Optional[str] = None
    is_active: Optional[bool] = None


class UserResponse(UserBase):
    id: str
    last_login: Optional[datetime] = None
    
    class Config:
        from_attributes = True


# 파이프라인 관련 모델
class PipelineBase(BaseModel):
    name: str
    description: Optional[str] = None
    config: Optional[Dict[str, Any]] = None


class PipelineCreate(PipelineBase):
    pass


class PipelineUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    config: Optional[Dict[str, Any]] = None


class PipelineResponse(PipelineBase):
    id: str
    created_at: datetime
    updated_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True


# 스케줄 관련 모델
class ScheduleBase(BaseModel):
    name: str
    pipeline_id: str
    cron_expression: str
    enabled: bool = True
    parameters: Optional[Dict[str, Any]] = None


class ScheduleCreate(ScheduleBase):
    pass


class ScheduleUpdate(BaseModel):
    name: Optional[str] = None
    pipeline_id: Optional[str] = None
    cron_expression: Optional[str] = None
    enabled: Optional[bool] = None
    parameters: Optional[Dict[str, Any]] = None


class ScheduleResponse(ScheduleBase):
    id: str
    created_at: datetime
    updated_at: Optional[datetime] = None
    
    model_config = {
        "from_attributes": True,
        "populate_by_name": True,
        "json_schema_extra": {
            "example": {
                "id": "b5a33cb9-3f1e-4e74-9d49-3f4ab6c2c2c6",
                "name": "일일 데이터 처리",
                "pipeline_id": "a1b2c3d4-5e6f-7g8h-9i0j-1k2l3m4n5o6p",
                "cron_expression": "0 0 * * *",
                "enabled": True,
                "parameters": {"date": "2023-01-01"},
                "created_at": "2023-01-01T00:00:00",
                "updated_at": "2023-01-02T00:00:00"
            }
        },
        "alias_generator": lambda name: "description" if name == "name" else name,
        "populate_by_alias": True,
    }


# 실행 이력 관련 모델
class ExecutionBase(BaseModel):
    pipeline_id: str
    status: str
    trigger: str
    logs: Optional[str] = None


class ExecutionCreate(ExecutionBase):
    pass


class ExecutionUpdate(BaseModel):
    status: Optional[str] = None
    logs: Optional[str] = None
    completed_at: Optional[datetime] = None


class ExecutionResponse(ExecutionBase):
    id: str
    started_at: datetime
    updated_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True


# 토큰 관련 모델
class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    refresh_token: Optional[str] = None


class TokenData(BaseModel):
    username: Optional[str] = None
    scopes: List[str] = []


# 인증 관련 모델
class LoginData(BaseModel):
    username: str
    password: str


# 메트릭 관련 모델
class MetricsSummary(BaseModel):
    total_pipelines: int
    active_schedules: int
    total_executions: int
    recent_success_rate: float  # 최근 24시간 실행 성공률
    pipeline_status: Dict[str, int]  # 각 상태별 파이프라인 수
    
    class Config:
        from_attributes = True


# 데이터소스 관련 모델
class DataSourceBase(BaseModel):
    name: str
    description: Optional[str] = None
    type: str  # mysql, postgres, bigquery, file 등
    config: Dict[str, Any]


class DataSourceCreate(DataSourceBase):
    pass


class DataSourceUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    config: Optional[Dict[str, Any]] = None


class DataSourceResponse(DataSourceBase):
    id: str
    created_at: datetime
    updated_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True


# 알림 관련 모델
class NotificationBase(BaseModel):
    user_id: str
    message: str
    type: str  # info, warning, error 등
    read: bool = False
    resource_type: Optional[str] = None  # pipeline, execution 등
    resource_id: Optional[str] = None


class NotificationCreate(NotificationBase):
    pass


class NotificationUpdate(BaseModel):
    read: Optional[bool] = None


class NotificationResponse(NotificationBase):
    id: str
    created_at: datetime
    
    class Config:
        from_attributes = True 
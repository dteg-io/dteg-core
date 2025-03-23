"""
DTEG Web API - 스케줄

스케줄 관리 엔드포인트
"""
from typing import List, Optional
import uuid
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Body
from pydantic import BaseModel
from sqlalchemy.orm import Session

from dteg.web.api.auth import get_current_active_user
from dteg.web.api.models import User, ScheduleCreate, ScheduleUpdate, ScheduleResponse
from dteg.web.models.database_models import Schedule, Pipeline
from dteg.web.database import get_db
from dteg.orchestration import get_orchestrator

# 라우터 정의
router = APIRouter()

@router.get("", response_model=List[ScheduleResponse])
async def get_schedules(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    스케줄 목록 조회
    
    Arguments:
        current_user: 현재 인증된 사용자 (의존성 주입)
        db: 데이터베이스 세션 (의존성 주입)
        
    Returns:
        List[ScheduleResponse]: 스케줄 목록
    """
    schedules = db.query(Schedule).all()
    return schedules

@router.get("/{schedule_id}", response_model=ScheduleResponse)
async def get_schedule(
    schedule_id: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    특정 스케줄 조회
    
    Arguments:
        schedule_id: 스케줄 ID
        current_user: 현재 인증된 사용자 (의존성 주입)
        db: 데이터베이스 세션 (의존성 주입)
        
    Returns:
        ScheduleResponse: 스케줄 정보
    """
    schedule = db.query(Schedule).filter(Schedule.id == schedule_id).first()
    
    if not schedule:
        raise HTTPException(status_code=404, detail="스케줄을 찾을 수 없습니다")
    
    return schedule

@router.post("", response_model=ScheduleResponse, status_code=201)
async def create_schedule(
    schedule: ScheduleCreate = Body(...),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    새 스케줄 생성
    
    Arguments:
        schedule: 생성할 스케줄 정보
        current_user: 현재 인증된 사용자 (의존성 주입)
        db: 데이터베이스 세션 (의존성 주입)
        
    Returns:
        ScheduleResponse: 생성된 스케줄 정보
    """
    # 파이프라인 존재 여부 확인
    pipeline = db.query(Pipeline).filter(Pipeline.id == schedule.pipeline_id).first()
    
    if not pipeline:
        raise HTTPException(status_code=404, detail="지정한 파이프라인을 찾을 수 없습니다")
    
    # 새 스케줄 생성
    new_schedule = Schedule(
        description=schedule.name,
        pipeline_id=schedule.pipeline_id,
        cron_expression=schedule.cron_expression,
        enabled=schedule.enabled,
        params=schedule.parameters
    )
    
    # 데이터베이스에 저장
    db.add(new_schedule)
    db.commit()
    db.refresh(new_schedule)
    
    # API 응답을 위해 name 필드 설정 (description 필드에서 가져옴)
    response_schedule = new_schedule
    
    # 스케줄러에 등록 시도
    try:
        orchestrator = get_orchestrator()
        orchestrator.schedule_pipeline(
            schedule_id=str(new_schedule.id),
            pipeline_id=schedule.pipeline_id,
            cron_expression=schedule.cron_expression,
            parameters=schedule.parameters
        )
        print(f"스케줄러에 등록 성공: {new_schedule.id}")
    except Exception as e:
        # 스케줄러 등록 실패 시에도 DB에는 저장은 유지하고 경고만 로그에 남김
        import logging
        logging.error(f"스케줄러 등록 실패: {str(e)}")
        print(f"스케줄러 등록 실패: {str(e)}")
    
    return response_schedule

@router.put("/{schedule_id}", response_model=ScheduleResponse)
async def update_schedule(
    schedule_id: str,
    schedule: ScheduleUpdate = Body(...),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    스케줄 정보 수정
    
    Arguments:
        schedule_id: 수정할 스케줄 ID
        schedule: 수정할 스케줄 정보
        current_user: 현재 인증된 사용자 (의존성 주입)
        db: 데이터베이스 세션 (의존성 주입)
        
    Returns:
        ScheduleResponse: 수정된 스케줄 정보
    """
    # 스케줄 존재 여부 확인
    db_schedule = db.query(Schedule).filter(Schedule.id == schedule_id).first()
    
    if not db_schedule:
        raise HTTPException(status_code=404, detail="스케줄을 찾을 수 없습니다")
    
    # 파이프라인 ID가 변경된 경우 존재 여부 확인
    if schedule.pipeline_id and schedule.pipeline_id != db_schedule.pipeline_id:
        pipeline = db.query(Pipeline).filter(Pipeline.id == schedule.pipeline_id).first()
        if not pipeline:
            raise HTTPException(status_code=404, detail="지정한 파이프라인을 찾을 수 없습니다")
    
    # 스케줄 업데이트
    if schedule.name is not None:
        db_schedule.name = schedule.name
    
    if schedule.pipeline_id is not None:
        db_schedule.pipeline_id = schedule.pipeline_id
        
    if schedule.cron_expression is not None:
        db_schedule.cron_expression = schedule.cron_expression
        
    if schedule.enabled is not None:
        db_schedule.enabled = schedule.enabled
        
    if schedule.parameters is not None:
        db_schedule.parameters = schedule.parameters
        
    db_schedule.updated_at = datetime.now()
    
    # 데이터베이스 업데이트
    db.commit()
    db.refresh(db_schedule)
    
    # 스케줄러 업데이트 시도
    if db_schedule.enabled:
        try:
            orchestrator = get_orchestrator()
            
            # 기존 스케줄 제거 후 재등록
            orchestrator.remove_schedule(schedule_id)
            
            # 활성화된 경우에만 재등록
            orchestrator.schedule_pipeline(
                schedule_id=str(db_schedule.id),
                pipeline_id=db_schedule.pipeline_id,
                cron_expression=db_schedule.cron_expression,
                parameters=db_schedule.parameters
            )
        except Exception as e:
            # 스케줄러 업데이트 실패 시에도 DB 업데이트는 유지
            print(f"스케줄러 업데이트 실패: {str(e)}")
    else:
        # 비활성화된 경우 스케줄에서 제거
        try:
            orchestrator = get_orchestrator()
            orchestrator.remove_schedule(schedule_id)
        except Exception as e:
            print(f"스케줄러에서 제거 실패: {str(e)}")
    
    return db_schedule

@router.delete("/{schedule_id}", status_code=204)
async def delete_schedule(
    schedule_id: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    스케줄 삭제
    
    Arguments:
        schedule_id: 삭제할 스케줄 ID
        current_user: 현재 인증된 사용자 (의존성 주입)
        db: 데이터베이스 세션 (의존성 주입)
        
    Returns:
        None: 204 No Content
    """
    # 스케줄 존재 여부 확인
    schedule = db.query(Schedule).filter(Schedule.id == schedule_id).first()
    
    if not schedule:
        raise HTTPException(status_code=404, detail="스케줄을 찾을 수 없습니다")
    
    # 스케줄러에서 제거 시도
    try:
        orchestrator = get_orchestrator()
        orchestrator.remove_schedule(schedule_id)
    except Exception as e:
        # 스케줄러에서 제거 실패 시에도 DB에서는 삭제 진행
        print(f"스케줄러에서 제거 실패: {str(e)}")
    
    # 데이터베이스에서 삭제
    db.delete(schedule)
    db.commit()
    
    return None 
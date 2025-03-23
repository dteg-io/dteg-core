"""
DTEG Web API - 대시보드

대시보드 데이터 엔드포인트
"""
from typing import Dict, List, Optional
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, desc

from dteg.web.api.auth import get_current_active_user
from dteg.web.api.models import User, MetricsSummary
from dteg.web.models.database_models import Pipeline, Schedule, Execution
from dteg.web.database import get_db

# 라우터 정의
router = APIRouter()

@router.get("/metrics", response_model=MetricsSummary)
async def get_metrics(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    대시보드 메트릭 요약 정보 조회
    
    Arguments:
        current_user: 현재 인증된 사용자 (의존성 주입)
        db: 데이터베이스 세션 (의존성 주입)
        
    Returns:
        MetricsSummary: 메트릭 요약 정보
    """
    # 파이프라인 개수
    total_pipelines = db.query(func.count(Pipeline.id)).scalar()
    
    # 활성화된 스케줄 개수
    active_schedules = db.query(func.count(Schedule.id)).filter(Schedule.enabled == True).scalar()
    
    # 총 실행 개수
    total_executions = db.query(func.count(Execution.id)).scalar()
    
    # 최근 24시간 실행 성공률
    one_day_ago = datetime.now() - timedelta(days=1)
    recent_executions = db.query(Execution).filter(Execution.started_at >= one_day_ago).all()
    
    # 성공률 계산
    if recent_executions:
        success_count = sum(1 for e in recent_executions if e.status == "completed")
        recent_success_rate = success_count / len(recent_executions) * 100
    else:
        recent_success_rate = 0
    
    # 상태별 파이프라인 수
    pipeline_status = {
        "completed": db.query(func.count(Execution.id)).filter(Execution.status == "completed").scalar(),
        "failed": db.query(func.count(Execution.id)).filter(Execution.status == "failed").scalar(),
        "running": db.query(func.count(Execution.id)).filter(Execution.status == "running").scalar(),
        "cancelled": db.query(func.count(Execution.id)).filter(Execution.status == "cancelled").scalar()
    }
    
    return MetricsSummary(
        total_pipelines=total_pipelines,
        active_schedules=active_schedules,
        total_executions=total_executions,
        recent_success_rate=recent_success_rate,
        pipeline_status=pipeline_status
    )

@router.get("/recent_executions")
async def get_recent_executions(
    limit: int = 5,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    최근 실행 이력 조회
    
    Arguments:
        limit: 조회할 최대 개수
        current_user: 현재 인증된 사용자 (의존성 주입)
        db: 데이터베이스 세션 (의존성 주입)
        
    Returns:
        List[dict]: 최근 실행 이력 목록
    """
    recent_executions = db.query(Execution).order_by(desc(Execution.started_at)).limit(limit).all()
    
    # 파이프라인 이름 조회를 위한 ID 목록
    pipeline_ids = [e.pipeline_id for e in recent_executions]
    
    # 파이프라인 정보 조회
    pipelines = {}
    if pipeline_ids:
        pipeline_records = db.query(Pipeline).filter(Pipeline.id.in_(pipeline_ids)).all()
        pipelines = {p.id: p.name for p in pipeline_records}
    
    # 응답 구성
    result = []
    for execution in recent_executions:
        pipeline_name = pipelines.get(execution.pipeline_id, "알 수 없음")
        
        result.append({
            "id": execution.id,
            "pipeline_id": execution.pipeline_id,
            "pipeline_name": pipeline_name,
            "status": execution.status,
            "created_at": execution.started_at,
            "completed_at": execution.ended_at,
            "trigger": execution.trigger
        })
    
    return result

@router.get("/execution_stats")
async def get_execution_stats(
    days: int = 7,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    기간별 실행 통계 조회
    
    Arguments:
        days: 조회할 일수 (기본 7일)
        current_user: 현재 인증된 사용자 (의존성 주입)
        db: 데이터베이스 세션 (의존성 주입)
        
    Returns:
        dict: 날짜별 실행 통계
    """
    start_date = datetime.now() - timedelta(days=days)
    
    # 날짜별 실행 통계를 담을 딕셔너리 초기화
    stats = {}
    
    # 날짜 범위 생성
    for i in range(days):
        date = (start_date + timedelta(days=i)).date()
        date_str = date.isoformat()
        stats[date_str] = {
            "total": 0,
            "completed": 0,
            "failed": 0,
            "cancelled": 0
        }
    
    # 날짜별 실행 통계 쿼리
    executions = db.query(Execution).filter(Execution.started_at >= start_date).all()
    
    # 날짜별 통계 계산
    for execution in executions:
        date_str = execution.started_at.date().isoformat()
        
        if date_str in stats:
            stats[date_str]["total"] += 1
            
            if execution.status == "completed":
                stats[date_str]["completed"] += 1
            elif execution.status == "failed":
                stats[date_str]["failed"] += 1
            elif execution.status == "cancelled":
                stats[date_str]["cancelled"] += 1
    
    # 날짜 순서대로 리스트 변환
    result = [
        {
            "date": date,
            "total": data["total"],
            "completed": data["completed"],
            "failed": data["failed"],
            "cancelled": data["cancelled"]
        }
        for date, data in sorted(stats.items())
    ]
    
    return result

@router.get("/pipeline_stats")
async def get_pipeline_stats(
    limit: int = 5,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    파이프라인별 실행 통계 조회
    
    Arguments:
        limit: 조회할 최대 파이프라인 수
        current_user: 현재 인증된 사용자 (의존성 주입)
        db: 데이터베이스 세션 (의존성 주입)
        
    Returns:
        List[dict]: 파이프라인별 실행 통계
    """
    # 파이프라인 목록 조회
    pipelines = db.query(Pipeline).all()
    
    # 파이프라인별 실행 통계
    pipeline_stats = []
    
    for pipeline in pipelines:
        # 해당 파이프라인의 실행 통계 조회
        executions = db.query(Execution).filter(Execution.pipeline_id == pipeline.id).all()
        
        if not executions:
            continue
        
        total = len(executions)
        completed = sum(1 for e in executions if e.status == "completed")
        failed = sum(1 for e in executions if e.status == "failed")
        
        success_rate = (completed / total * 100) if total > 0 else 0
        
        # 가장 최근 실행
        recent_execution = max(executions, key=lambda e: e.started_at if e.started_at else datetime.min)
        
        pipeline_stats.append({
            "id": pipeline.id,
            "name": pipeline.name,
            "total_executions": total,
            "success_rate": success_rate,
            "last_execution": {
                "id": recent_execution.id,
                "status": recent_execution.status,
                "created_at": recent_execution.started_at,
                "completed_at": recent_execution.ended_at
            }
        })
    
    # 총 실행 횟수 기준으로 정렬하고 limit 적용
    pipeline_stats.sort(key=lambda x: x["total_executions"], reverse=True)
    return pipeline_stats[:limit] 
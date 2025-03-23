"""
DTEG Web API - 실행 이력

파이프라인 실행 이력 관련 엔드포인트
"""
from typing import List, Optional
import uuid
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import desc

from dteg.web.api.auth import get_current_active_user
from dteg.web.api.models import User, ExecutionResponse
from dteg.web.models.database_models import Execution, Pipeline
from dteg.web.database import get_db
from dteg.orchestration import get_orchestrator

# 라우터 정의
router = APIRouter()

@router.get("", response_model=List[ExecutionResponse])
async def get_executions(
    pipeline_id: Optional[str] = None, 
    status: Optional[str] = None,
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0, ge=0),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    실행 이력 목록 조회
    
    Arguments:
        pipeline_id: 필터링할 파이프라인 ID (선택)
        status: 필터링할 상태 (선택)
        limit: 반환할 최대 실행 이력 수
        offset: 건너뛸 실행 이력 수
        current_user: 현재 인증된 사용자 (의존성 주입)
        db: 데이터베이스 세션 (의존성 주입)
        
    Returns:
        List[ExecutionResponse]: 실행 이력 목록
    """
    query = db.query(Execution)
    
    # 필터 적용
    if pipeline_id:
        query = query.filter(Execution.pipeline_id == pipeline_id)
    
    if status:
        query = query.filter(Execution.status == status)
    
    # 최신 순으로 정렬
    query = query.order_by(desc(Execution.started_at))
    
    # 페이지네이션 적용
    executions = query.offset(offset).limit(limit).all()
    
    return executions

@router.get("/{execution_id}", response_model=ExecutionResponse)
async def get_execution(
    execution_id: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    특정 실행 이력 조회
    
    Arguments:
        execution_id: 실행 이력 ID
        current_user: 현재 인증된 사용자 (의존성 주입)
        db: 데이터베이스 세션 (의존성 주입)
        
    Returns:
        ExecutionResponse: 실행 이력 정보
    """
    execution = db.query(Execution).filter(Execution.id == execution_id).first()
    
    if not execution:
        raise HTTPException(status_code=404, detail="실행 이력을 찾을 수 없습니다")
    
    return execution

@router.get("/{execution_id}/logs", response_model=dict)
async def get_execution_logs(
    execution_id: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    특정 실행의 로그 조회
    
    Arguments:
        execution_id: 실행 이력 ID
        current_user: 현재 인증된 사용자 (의존성 주입)
        db: 데이터베이스 세션 (의존성 주입)
        
    Returns:
        dict: 로그 정보 (로그 텍스트, 상태 등)
    """
    execution = db.query(Execution).filter(Execution.id == execution_id).first()
    
    if not execution:
        raise HTTPException(status_code=404, detail="실행 이력을 찾을 수 없습니다")
    
    # 로그가 없을 경우 기본 메시지
    logs = execution.logs or "로그 정보가 없습니다."
    
    # 실행 중인 경우 추가 로그 시도
    if execution.status == "running":
        try:
            orchestrator = get_orchestrator()
            additional_logs = orchestrator.get_execution_logs(execution_id)
            
            if additional_logs:
                # 로그 업데이트
                if isinstance(additional_logs, str):
                    logs = additional_logs
                elif isinstance(additional_logs, dict) and "logs" in additional_logs:
                    logs = additional_logs["logs"]
                
                # 실행 중인 경우만 DB 로그 업데이트
                execution.logs = logs
                db.commit()
                
                # 실행 상태 업데이트 (완료/실패 여부)
                if additional_logs.get("status") in ["completed", "failed"]:
                    execution.status = additional_logs["status"]
                    execution.ended_at = datetime.now()
                    db.commit()
        except Exception as e:
            # 로그 검색 실패 시에도 기존 로그는 반환
            print(f"로그 검색 실패: {str(e)}")
    
    return {
        "execution_id": execution_id,
        "status": execution.status,
        "logs": logs,
        "pipeline_id": execution.pipeline_id,
        "created_at": execution.started_at,
        "completed_at": execution.ended_at
    }

@router.delete("/{execution_id}", status_code=204)
async def delete_execution(
    execution_id: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    실행 이력 삭제
    
    Arguments:
        execution_id: 삭제할 실행 이력 ID
        current_user: 현재 인증된 사용자 (의존성 주입)
        db: 데이터베이스 세션 (의존성 주입)
        
    Returns:
        None: 204 No Content
    """
    execution = db.query(Execution).filter(Execution.id == execution_id).first()
    
    if not execution:
        raise HTTPException(status_code=404, detail="실행 이력을 찾을 수 없습니다")
    
    # 실행 중인 경우 취소 시도
    if execution.status == "running":
        try:
            orchestrator = get_orchestrator()
            orchestrator.cancel_execution(execution_id)
        except Exception as e:
            # 취소 실패 시에도 DB에서는 삭제 진행
            print(f"실행 취소 실패: {str(e)}")
    
    # 데이터베이스에서 삭제
    db.delete(execution)
    db.commit()
    
    return None

@router.post("/{execution_id}/cancel", status_code=200)
async def cancel_execution(
    execution_id: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    실행 취소
    
    Arguments:
        execution_id: 취소할 실행 ID
        current_user: 현재 인증된 사용자 (의존성 주입)
        db: 데이터베이스 세션 (의존성 주입)
        
    Returns:
        dict: 취소 상태 정보
    """
    execution = db.query(Execution).filter(Execution.id == execution_id).first()
    
    if not execution:
        raise HTTPException(status_code=404, detail="실행 이력을 찾을 수 없습니다")
    
    # 이미 완료되었거나 실패한 경우
    if execution.status in ["completed", "failed", "cancelled"]:
        raise HTTPException(
            status_code=400, 
            detail=f"이미 {execution.status} 상태인 실행은 취소할 수 없습니다"
        )
    
    # 실행 취소 시도
    try:
        orchestrator = get_orchestrator()
        result = orchestrator.cancel_execution(execution_id)
        
        # 상태 업데이트
        execution.status = "cancelled"
        execution.ended_at = datetime.now()
        execution.logs = (execution.logs or "") + "\n실행이 취소되었습니다."
        db.commit()
        
        return {
            "status": "success",
            "message": "실행이 취소되었습니다",
            "execution_id": execution_id
        }
    except Exception as e:
        # 취소 실패
        raise HTTPException(
            status_code=500,
            detail=f"실행 취소 중 오류가 발생했습니다: {str(e)}"
        ) 
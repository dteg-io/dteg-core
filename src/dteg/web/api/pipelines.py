"""
DTEG Web API - 파이프라인

파이프라인 관련 엔드포인트
"""
from typing import List, Optional
import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Body
from pydantic import UUID4
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from dteg.web.api.auth import get_current_active_user
from dteg.web.api.models import User, PipelineCreate, PipelineUpdate, PipelineResponse
from dteg.web.models.database_models import Pipeline
from dteg.web.database import get_db
from dteg.orchestration import get_orchestrator

# 라우터 정의
router = APIRouter()

@router.get("", response_model=List[PipelineResponse])
async def get_pipelines(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    파이프라인 목록 조회
    
    Arguments:
        current_user: 현재 인증된 사용자 (의존성 주입)
        db: 데이터베이스 세션 (의존성 주입)
        
    Returns:
        List[PipelineResponse]: 파이프라인 목록
    """
    pipelines = db.query(Pipeline).all()
    return pipelines

@router.get("/{pipeline_id}", response_model=PipelineResponse)
async def get_pipeline(
    pipeline_id: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    특정 파이프라인 조회
    
    Arguments:
        pipeline_id: 파이프라인 ID
        current_user: 현재 인증된 사용자 (의존성 주입)
        db: 데이터베이스 세션 (의존성 주입)
        
    Returns:
        PipelineResponse: 파이프라인 정보
    """
    # 파이프라인 ID로 검색
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    
    if not pipeline:
        # 파이프라인을 찾지 못한 경우 404 오류 반환
        raise HTTPException(status_code=404, detail="파이프라인을 찾을 수 없습니다")
    
    return pipeline

@router.post("", response_model=PipelineResponse, status_code=201)
async def create_pipeline(
    pipeline: PipelineCreate = Body(...),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    새 파이프라인 생성
    
    Arguments:
        pipeline: 생성할 파이프라인 정보
        current_user: 현재 인증된 사용자 (의존성 주입)
        db: 데이터베이스 세션 (의존성 주입)
        
    Returns:
        PipelineResponse: 생성된 파이프라인 정보
    """
    # 새 파이프라인 데이터 생성
    new_pipeline = Pipeline(
        name=pipeline.name,
        description=pipeline.description,
        config=pipeline.config
    )
    
    # 데이터베이스에 추가
    db.add(new_pipeline)
    db.commit()
    db.refresh(new_pipeline)
    
    return new_pipeline

@router.put("/{pipeline_id}", response_model=PipelineResponse)
async def update_pipeline(
    pipeline_id: str,
    pipeline: PipelineUpdate = Body(...),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    파이프라인 정보 수정
    
    Arguments:
        pipeline_id: 수정할 파이프라인 ID
        pipeline: 수정할 파이프라인 정보
        current_user: 현재 인증된 사용자 (의존성 주입)
        db: 데이터베이스 세션 (의존성 주입)
        
    Returns:
        PipelineResponse: 수정된 파이프라인 정보
    """
    # 파이프라인 ID로 검색
    db_pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    
    if not db_pipeline:
        # 파이프라인을 찾지 못한 경우 404 오류 반환
        raise HTTPException(status_code=404, detail="파이프라인을 찾을 수 없습니다")
    
    # 파이프라인 정보 업데이트
    db_pipeline.name = pipeline.name
    db_pipeline.description = pipeline.description
    db_pipeline.updated_at = datetime.now()
    
    if pipeline.config:
        db_pipeline.config = pipeline.config
    
    db.commit()
    db.refresh(db_pipeline)
    
    return db_pipeline

@router.delete("/{pipeline_id}", status_code=204)
async def delete_pipeline(
    pipeline_id: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    파이프라인 삭제
    
    Arguments:
        pipeline_id: 삭제할 파이프라인 ID
        current_user: 현재 인증된 사용자 (의존성 주입)
        db: 데이터베이스 세션 (의존성 주입)
        
    Returns:
        None: 204 No Content
    """
    # 파이프라인 ID로 검색
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    
    if not pipeline:
        # 파이프라인을 찾지 못한 경우 404 오류 반환
        raise HTTPException(status_code=404, detail="파이프라인을 찾을 수 없습니다")
    
    # 파이프라인 삭제
    db.delete(pipeline)
    db.commit()
    
    # 204 No Content 반환
    return None

@router.post("/{pipeline_id}/run", status_code=202)
async def run_pipeline(
    pipeline_id: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    파이프라인 실행
    
    Arguments:
        pipeline_id: 실행할 파이프라인 ID
        current_user: 현재 인증된 사용자 (의존성 주입)
        db: 데이터베이스 세션 (의존성 주입)
        
    Returns:
        dict: 실행 상태 정보
    """
    # 파이프라인 존재 여부 확인
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    
    if not pipeline:
        raise HTTPException(status_code=404, detail="파이프라인을 찾을 수 없습니다")
    
    try:
        # 오케스트레이터를 통해 파이프라인 실행
        orchestrator = get_orchestrator()
        
        # 파이프라인 실행 (실제 구현에서는 파이프라인 ID 또는 설정 전달)
        result = orchestrator.run_pipeline(
            pipeline_id=pipeline_id,
            config=pipeline.config,
            async_execution=True
        )
        
        # 실행 정보가 있는 경우 execution_id 추출
        execution_id = result.get("execution_id", str(uuid.uuid4()))
        
        # 로그에 실행 시작 기록
        from dteg.web.models.database_models import Execution
        
        # 실행 이력 생성
        execution = Execution(
            id=execution_id,
            pipeline_id=pipeline_id,
            status="running",
            trigger="manual",
            logs="파이프라인 실행 시작..."
        )
        
        db.add(execution)
        db.commit()
        
        # 실행 요청 성공 응답
        return {
            "status": "success",
            "message": "파이프라인 실행이 예약되었습니다",
            "execution_id": execution_id,
            "pipeline_id": pipeline_id
        }
        
    except Exception as e:
        # 실행 중 오류 발생 시
        raise HTTPException(
            status_code=500,
            detail=f"파이프라인 실행 중 오류가 발생했습니다: {str(e)}"
        ) 
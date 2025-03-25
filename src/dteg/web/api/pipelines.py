"""
DTEG Web API - 파이프라인

파이프라인 관련 엔드포인트
"""
from typing import List, Optional, Dict
import uuid
import os
import json
import glob
import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Body
from pydantic import UUID4
from fastapi.responses import JSONResponse

from dteg.web.api.auth import get_current_active_user
from dteg.web.api.models import User, PipelineCreate, PipelineUpdate
from dteg.orchestration import get_orchestrator
from dteg.config import get_config

# 로거 설정
logger = logging.getLogger(__name__)

# 라우터 정의
router = APIRouter()

@router.get("", response_model=List[Dict])
async def get_pipelines(
    current_user: User = Depends(get_current_active_user)
):
    """
    파이프라인 목록 조회
    
    Arguments:
        current_user: 현재 인증된 사용자 (의존성 주입)
        
    Returns:
        List[Dict]: 파이프라인 목록
    """
    config = get_config()
    pipelines_dir = config.pipelines_dir
    
    logger.info(f"파이프라인 목록 조회: 디렉토리={pipelines_dir}")
    
    # 디렉토리 존재 확인
    if not os.path.exists(pipelines_dir):
        logger.warning(f"파이프라인 디렉토리가 존재하지 않습니다: {pipelines_dir}")
        os.makedirs(pipelines_dir, exist_ok=True)
        return []
    
    # 모든 파이프라인 파일 로드
    pipeline_files = glob.glob(os.path.join(pipelines_dir, "*.json"))
    logger.info(f"발견된 파이프라인 파일 수: {len(pipeline_files)}")
    
    pipelines = []
    
    for pipeline_file in pipeline_files:
        try:
            with open(pipeline_file, 'r') as f:
                pipeline = json.load(f)
                pipelines.append(pipeline)
        except Exception as e:
            logger.error(f"파이프라인 파일 읽기 오류: {str(e)}")
    
    return pipelines

@router.get("/{pipeline_id}", response_model=Dict)
async def get_pipeline(
    pipeline_id: str,
    current_user: User = Depends(get_current_active_user)
):
    """
    특정 파이프라인 조회
    
    Arguments:
        pipeline_id: 파이프라인 ID
        current_user: 현재 인증된 사용자 (의존성 주입)
        
    Returns:
        Dict: 파이프라인 정보
    """
    config = get_config()
    pipeline_file = os.path.join(config.pipelines_dir, f"{pipeline_id}.json")
    
    if not os.path.exists(pipeline_file):
        # 파이프라인을 찾지 못한 경우 404 오류 반환
        raise HTTPException(status_code=404, detail="파이프라인을 찾을 수 없습니다")
    
    try:
        with open(pipeline_file, 'r') as f:
            pipeline = json.load(f)
            return pipeline
    except Exception as e:
        logger.error(f"파이프라인 파일 읽기 오류: {str(e)}")
        raise HTTPException(status_code=500, detail="파이프라인 로드 중 오류가 발생했습니다")

@router.post("", response_model=Dict, status_code=201)
async def create_pipeline(
    pipeline: PipelineCreate = Body(...),
    current_user: User = Depends(get_current_active_user)
):
    """
    새 파이프라인 생성
    
    Arguments:
        pipeline: 생성할 파이프라인 정보
        current_user: 현재 인증된 사용자 (의존성 주입)
        
    Returns:
        Dict: 생성된 파이프라인 정보
    """
    config = get_config()
    pipelines_dir = config.pipelines_dir
    
    # 디렉토리 존재 확인
    if not os.path.exists(pipelines_dir):
        os.makedirs(pipelines_dir, exist_ok=True)
    
    # 새 파이프라인 ID 생성
    pipeline_id = str(uuid.uuid4())
    
    # 현재 시간
    now = datetime.now().isoformat()
    
    # 파이프라인 데이터 생성
    pipeline_data = {
        "id": pipeline_id,
        "name": pipeline.name,
        "description": pipeline.description,
        "config": pipeline.config,
        "created_at": now,
        "updated_at": now
    }
    
    # 파일에 저장
    pipeline_file = os.path.join(pipelines_dir, f"{pipeline_id}.json")
    
    try:
        with open(pipeline_file, 'w') as f:
            json.dump(pipeline_data, f, indent=2)
    except Exception as e:
        logger.error(f"파이프라인 파일 저장 오류: {str(e)}")
        raise HTTPException(status_code=500, detail="파이프라인 저장 중 오류가 발생했습니다")
    
    return pipeline_data

@router.put("/{pipeline_id}", response_model=Dict)
async def update_pipeline(
    pipeline_id: str,
    pipeline: PipelineUpdate = Body(...),
    current_user: User = Depends(get_current_active_user)
):
    """
    파이프라인 정보 수정
    
    Arguments:
        pipeline_id: 수정할 파이프라인 ID
        pipeline: 수정할 파이프라인 정보
        current_user: 현재 인증된 사용자 (의존성 주입)
        
    Returns:
        Dict: 수정된 파이프라인 정보
    """
    config = get_config()
    pipeline_file = os.path.join(config.pipelines_dir, f"{pipeline_id}.json")
    
    if not os.path.exists(pipeline_file):
        # 파이프라인을 찾지 못한 경우 404 오류 반환
        raise HTTPException(status_code=404, detail="파이프라인을 찾을 수 없습니다")
    
    try:
        with open(pipeline_file, 'r') as f:
            pipeline_data = json.load(f)
        
        # 파이프라인 정보 업데이트
        pipeline_data["name"] = pipeline.name
        pipeline_data["description"] = pipeline.description
        pipeline_data["updated_at"] = datetime.now().isoformat()
        
        if pipeline.config:
            pipeline_data["config"] = pipeline.config
        
        # 파일에 저장
        with open(pipeline_file, 'w') as f:
            json.dump(pipeline_data, f, indent=2)
        
        return pipeline_data
    except Exception as e:
        logger.error(f"파이프라인 파일 업데이트 오류: {str(e)}")
        raise HTTPException(status_code=500, detail="파이프라인 업데이트 중 오류가 발생했습니다")

@router.delete("/{pipeline_id}", status_code=204)
async def delete_pipeline(
    pipeline_id: str,
    current_user: User = Depends(get_current_active_user)
):
    """
    파이프라인 삭제
    
    Arguments:
        pipeline_id: 삭제할 파이프라인 ID
        current_user: 현재 인증된 사용자 (의존성 주입)
        
    Returns:
        None: 204 No Content
    """
    config = get_config()
    pipeline_file = os.path.join(config.pipelines_dir, f"{pipeline_id}.json")
    
    if not os.path.exists(pipeline_file):
        # 파이프라인을 찾지 못한 경우 404 오류 반환
        raise HTTPException(status_code=404, detail="파이프라인을 찾을 수 없습니다")
    
    try:
        # 파이프라인 파일 삭제
        os.remove(pipeline_file)
        return None
    except Exception as e:
        logger.error(f"파이프라인 파일 삭제 오류: {str(e)}")
        raise HTTPException(status_code=500, detail="파이프라인 삭제 중 오류가 발생했습니다")

@router.post("/{pipeline_id}/run", status_code=202)
async def run_pipeline(
    pipeline_id: str,
    current_user: User = Depends(get_current_active_user)
):
    """
    파이프라인 실행
    
    Arguments:
        pipeline_id: 실행할 파이프라인 ID
        current_user: 현재 인증된 사용자 (의존성 주입)
        
    Returns:
        dict: 실행 상태 정보
    """
    config = get_config()
    pipeline_file = os.path.join(config.pipelines_dir, f"{pipeline_id}.json")
    
    if not os.path.exists(pipeline_file):
        raise HTTPException(status_code=404, detail="파이프라인을 찾을 수 없습니다")
    
    try:
        # 파이프라인 파일 읽기
        with open(pipeline_file, 'r') as f:
            pipeline = json.load(f)
        
        # 오케스트레이터를 통해 파이프라인 실행
        orchestrator = get_orchestrator()
        
        # 파이프라인 실행
        result = orchestrator.run_pipeline(
            pipeline_id=pipeline_id,
            config=pipeline.get("config", {}),
            async_execution=True
        )
        
        # 실행 정보가 있는 경우 execution_id 추출
        execution_id = result.get("execution_id", str(uuid.uuid4()))
        
        # 실행 이력 생성
        execution_data = {
            "id": execution_id,
            "pipeline_id": pipeline_id,
            "status": "running",
            "trigger": "manual",
            "started_at": datetime.now().isoformat(),
            "logs": "파이프라인 실행 시작..."
        }
        
        # 실행 이력 저장
        execution_file = os.path.join(config.executions_dir, f"{execution_id}.json")
        with open(execution_file, 'w') as f:
            json.dump(execution_data, f, indent=2)
        
        # 실행 요청 성공 응답
        return {
            "status": "success",
            "message": "파이프라인 실행이 예약되었습니다",
            "execution_id": execution_id,
            "pipeline_id": pipeline_id
        }
        
    except Exception as e:
        logger.error(f"파이프라인 실행 오류: {str(e)}")
        # 실행 중 오류 발생 시
        raise HTTPException(
            status_code=500,
            detail=f"파이프라인 실행 중 오류가 발생했습니다: {str(e)}"
        ) 
"""
DTEG Web API - 실행 이력

파이프라인 실행 이력 관련 엔드포인트
"""
from typing import List, Optional
import uuid
import os
import json
import glob
import logging
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from dteg.web.api.auth import get_current_active_user
from dteg.web.api.models import User, ExecutionResponse, ExecutionListResponse
from dteg.config import get_config
from dteg.orchestration import get_orchestrator

# 로거 설정
logger = logging.getLogger(__name__)

# 라우터 정의
router = APIRouter()

@router.get("", response_model=List[dict])
async def get_executions(
    pipeline_id: Optional[str] = None, 
    status: Optional[str] = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, le=100),
    current_user: User = Depends(get_current_active_user)
):
    """
    실행 이력 목록 조회
    
    Arguments:
        pipeline_id: 필터링할 파이프라인 ID (선택)
        status: 필터링할 상태 (선택)
        page: 페이지 번호 (1부터 시작)
        page_size: 페이지 크기 (한 페이지당 항목 수)
        current_user: 현재 인증된 사용자 (의존성 주입)
        
    Returns:
        List[dict]: 실행 이력 목록
    """
    config = get_config()
    executions_dir = os.path.join(config.storage_path, "executions")
    
    # 로깅 추가
    logger.info(f"실행 이력 조회: executions_dir={executions_dir}")
    logger.info(f"디렉토리 존재 여부: {os.path.exists(executions_dir)}")

    # 모든 실행 이력 파일 로드
    execution_files = glob.glob(os.path.join(executions_dir, "*.json"))
    logger.info(f"발견된 실행 이력 파일 수: {len(execution_files)}")
    
    executions = []
    
    for execution_file in execution_files:
        try:
            with open(execution_file, 'r') as f:
                execution = json.load(f)
                
                # 필터 적용
                if pipeline_id and execution.get("pipeline_id") != pipeline_id:
                    continue
                
                if status and execution.get("status", "").lower() != status.lower():
                    continue
                
                # 시작 시간 파싱 (정렬용)
                if "started_at" in execution:
                    try:
                        started_at = datetime.fromisoformat(execution["started_at"].replace("Z", "+00:00"))
                        execution["started_at_datetime"] = started_at
                    except Exception:
                        execution["started_at_datetime"] = datetime.min
                else:
                    execution["started_at_datetime"] = datetime.min
                
                executions.append(execution)
        except Exception as e:
            logger.error(f"실행 이력 파일 읽기 오류: {str(e)}")
    
    # 최신 순으로 정렬
    executions.sort(key=lambda x: x["started_at_datetime"], reverse=True)
    
    # 전체 개수 계산
    total = len(executions)
    logger.info(f"필터링 후 실행 이력 수: {total}")
    
    # 페이지네이션 적용
    offset = (page - 1) * page_size
    paginated_executions = executions[offset:offset + page_size] if executions else []
    
    # 응답에서 시작 시간 datetime 객체 제거
    for execution in paginated_executions:
        if "started_at_datetime" in execution:
            del execution["started_at_datetime"]
    
    return paginated_executions

@router.get("/{execution_id}", response_model=dict)
async def get_execution(
    execution_id: str,
    current_user: User = Depends(get_current_active_user)
):
    """
    특정 실행 이력 조회
    
    Arguments:
        execution_id: 실행 이력 ID
        current_user: 현재 인증된 사용자 (의존성 주입)
        
    Returns:
        dict: 실행 이력 정보
    """
    config = get_config()
    execution_file = os.path.join(config.storage_path, "executions", f"{execution_id}.json")
    
    if not os.path.exists(execution_file):
        raise HTTPException(status_code=404, detail="실행 이력을 찾을 수 없습니다")
    
    try:
        with open(execution_file, 'r') as f:
            execution = json.load(f)
            return execution
    except Exception as e:
        logger.error(f"실행 이력 파일 읽기 오류: {str(e)}")
        raise HTTPException(status_code=500, detail="실행 이력 로드 중 오류가 발생했습니다")

@router.get("/{execution_id}/logs", response_model=dict)
async def get_execution_logs(
    execution_id: str,
    current_user: User = Depends(get_current_active_user)
):
    """
    실행 로그 조회
    
    Arguments:
        execution_id: 실행 이력 ID
        current_user: 현재 인증된 사용자 (의존성 주입)
        
    Returns:
        dict: 실행 로그 정보
    """
    config = get_config()
    execution_file = os.path.join(config.storage_path, "executions", f"{execution_id}.json")
    
    if not os.path.exists(execution_file):
        raise HTTPException(status_code=404, detail="실행 이력을 찾을 수 없습니다")
    
    # 실행 파일 로드
    try:
        with open(execution_file, 'r') as f:
            execution = json.load(f)
    except Exception as e:
        logger.error(f"실행 이력 파일 읽기 오류: {str(e)}")
        raise HTTPException(status_code=500, detail="실행 이력 로드 중 오류가 발생했습니다")
    
    # 로그 파일 경로
    log_file = os.path.join(config.storage_path, "logs", f"execution_{execution_id}.log")
    logs = ""
    
    if os.path.exists(log_file):
        try:
            with open(log_file, 'r') as f:
                logs = f.read()
        except Exception as e:
            logger.error(f"로그 파일 읽기 오류: {str(e)}")
            logs = "로그 파일 읽기 오류가 발생했습니다."
    else:
        logs = "로그 파일이 존재하지 않습니다."
    
    return {
        "execution_id": execution_id,
        "status": execution.get("status", "unknown"),
        "logs": logs,
        "pipeline_id": execution.get("pipeline_id", ""),
        "created_at": execution.get("started_at", ""),
        "completed_at": execution.get("ended_at", "")
    }

@router.delete("/{execution_id}", response_model=dict)
async def delete_execution(
    execution_id: str,
    current_user: User = Depends(get_current_active_user)
):
    """
    실행 이력 삭제
    
    Arguments:
        execution_id: 삭제할 실행 이력 ID
        current_user: 현재 인증된 사용자 (의존성 주입)
        
    Returns:
        dict: 삭제 결과
    """
    config = get_config()
    execution_file = os.path.join(config.storage_path, "executions", f"{execution_id}.json")
    
    if not os.path.exists(execution_file):
        raise HTTPException(status_code=404, detail="실행 이력을 찾을 수 없습니다")
    
    # 실행 파일 삭제
    try:
        os.remove(execution_file)
    except Exception as e:
        logger.error(f"실행 이력 파일 삭제 오류: {str(e)}")
        raise HTTPException(status_code=500, detail="실행 이력 삭제 중 오류가 발생했습니다")
    
    # 로그 파일도 삭제
    log_file = os.path.join(config.storage_path, "logs", f"execution_{execution_id}.log")
    if os.path.exists(log_file):
        try:
            os.remove(log_file)
        except Exception as e:
            logger.error(f"로그 파일 삭제 오류: {str(e)}")
    
    return {"message": "실행 이력이 삭제되었습니다", "execution_id": execution_id} 
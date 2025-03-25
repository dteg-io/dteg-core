"""
DTEG Web API - 스케줄

스케줄 관리 엔드포인트
"""
from typing import List, Optional, Dict
import uuid
import os
import json
import glob
import logging
from datetime import datetime, timedelta
import croniter

from fastapi import APIRouter, Depends, HTTPException, Body
from pydantic import BaseModel

from dteg.web.api.auth import get_current_active_user
from dteg.web.api.models import User, ScheduleCreate, ScheduleUpdate
from dteg.orchestration import get_orchestrator
from dteg.config import get_config

# 로거 설정
logger = logging.getLogger(__name__)

# 라우터 정의
router = APIRouter()

def calculate_next_run(cron_expression: str, now=None):
    """
    cron 표현식에서 다음 실행 시간을 계산
    
    Args:
        cron_expression: cron 표현식
        now: 기준 시간 (None이면 현재 시간)
        
    Returns:
        str: ISO 형식의 다음 실행 시간
    """
    try:
        now = now or datetime.now()
        cron = croniter.croniter(cron_expression, now)
        next_run = cron.get_next(datetime)
        return next_run.isoformat()
    except Exception as e:
        logger.error(f"다음 실행 시간 계산 오류: {str(e)}")
        # 기본값으로 하루 후 반환
        return (now + timedelta(days=1)).isoformat()

@router.get("", response_model=List[Dict])
async def get_schedules(
    current_user: User = Depends(get_current_active_user)
):
    """
    스케줄 목록 조회
    
    Arguments:
        current_user: 현재 인증된 사용자 (의존성 주입)
        
    Returns:
        List[Dict]: 스케줄 목록
    """
    config = get_config()
    schedules_dir = config.schedules_dir
    
    logger.info(f"스케줄 목록 조회: 디렉토리={schedules_dir}")
    
    # 디렉토리 존재 확인
    if not os.path.exists(schedules_dir):
        logger.warning(f"스케줄 디렉토리가 존재하지 않습니다: {schedules_dir}")
        os.makedirs(schedules_dir, exist_ok=True)
        return []
    
    # 모든 스케줄 파일 로드
    schedule_files = glob.glob(os.path.join(schedules_dir, "*.json"))
    logger.info(f"발견된 스케줄 파일 수: {len(schedule_files)}")
    
    schedules = []
    
    for schedule_file in schedule_files:
        try:
            with open(schedule_file, 'r') as f:
                schedule = json.load(f)
                # name 필드 호환성 유지
                if "name" not in schedule and "description" in schedule:
                    schedule["name"] = schedule["description"]
                schedules.append(schedule)
        except Exception as e:
            logger.error(f"스케줄 파일 읽기 오류: {str(e)}")
    
    return schedules

@router.get("/{schedule_id}", response_model=Dict)
async def get_schedule(
    schedule_id: str,
    current_user: User = Depends(get_current_active_user)
):
    """
    특정 스케줄 조회
    
    Arguments:
        schedule_id: 스케줄 ID
        current_user: 현재 인증된 사용자 (의존성 주입)
        
    Returns:
        Dict: 스케줄 정보
    """
    config = get_config()
    schedule_file = os.path.join(config.schedules_dir, f"{schedule_id}.json")
    
    if not os.path.exists(schedule_file):
        raise HTTPException(status_code=404, detail="스케줄을 찾을 수 없습니다")
    
    try:
        with open(schedule_file, 'r') as f:
            schedule = json.load(f)
            # name 필드 호환성 유지
            if "name" not in schedule and "description" in schedule:
                schedule["name"] = schedule["description"]
            return schedule
    except Exception as e:
        logger.error(f"스케줄 파일 읽기 오류: {str(e)}")
        raise HTTPException(status_code=500, detail="스케줄 로드 중 오류가 발생했습니다")

@router.post("", response_model=Dict, status_code=201)
async def create_schedule(
    schedule: ScheduleCreate = Body(...),
    current_user: User = Depends(get_current_active_user)
):
    """
    새 스케줄 생성
    
    Arguments:
        schedule: 생성할 스케줄 정보
        current_user: 현재 인증된 사용자 (의존성 주입)
        
    Returns:
        Dict: 생성된 스케줄 정보
    """
    config = get_config()
    schedules_dir = config.schedules_dir
    pipelines_dir = config.pipelines_dir
    
    # 디렉토리 존재 확인
    if not os.path.exists(schedules_dir):
        os.makedirs(schedules_dir, exist_ok=True)
    
    # 파이프라인 존재 여부 확인
    pipeline_file = os.path.join(pipelines_dir, f"{schedule.pipeline_id}.json")
    if not os.path.exists(pipeline_file):
        raise HTTPException(status_code=404, detail="지정한 파이프라인을 찾을 수 없습니다")
    
    # 새 스케줄 ID 생성
    schedule_id = str(uuid.uuid4())
    
    # 현재 시간
    now = datetime.now()
    
    # 다음 실행 시간 계산
    next_run = calculate_next_run(schedule.cron_expression, now)
    
    # 스케줄 데이터 생성
    schedule_data = {
        "id": schedule_id,
        "name": schedule.name,
        "description": schedule.name,  # 호환성을 위해 두 필드 모두 저장
        "pipeline_id": schedule.pipeline_id,
        "cron_expression": schedule.cron_expression,
        "enabled": schedule.enabled,
        "parameters": schedule.parameters,  # 원래 이름대로 저장
        "params": schedule.parameters,      # 호환성을 위해 두 필드 모두 저장
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
        "next_run": next_run
    }
    
    # 파일에 저장
    schedule_file = os.path.join(schedules_dir, f"{schedule_id}.json")
    
    try:
        with open(schedule_file, 'w') as f:
            json.dump(schedule_data, f, indent=2)
    except Exception as e:
        logger.error(f"스케줄 파일 저장 오류: {str(e)}")
        raise HTTPException(status_code=500, detail="스케줄 저장 중 오류가 발생했습니다")
    
    # 스케줄러에 등록 시도
    if schedule.enabled:
        try:
            orchestrator = get_orchestrator()
            orchestrator.schedule_pipeline(
                schedule_id=schedule_id,
                pipeline_id=schedule.pipeline_id,
                cron_expression=schedule.cron_expression,
                parameters=schedule.parameters
            )
            logger.info(f"스케줄러에 등록 성공: {schedule_id}")
        except Exception as e:
            # 스케줄러 등록 실패 시에도 파일 저장은 유지하고 경고만 로그에 남김
            logger.error(f"스케줄러 등록 실패: {str(e)}")
    
    return schedule_data

@router.put("/{schedule_id}", response_model=Dict)
async def update_schedule(
    schedule_id: str,
    schedule: ScheduleUpdate = Body(...),
    current_user: User = Depends(get_current_active_user)
):
    """
    스케줄 정보 수정
    
    Arguments:
        schedule_id: 수정할 스케줄 ID
        schedule: 수정할 스케줄 정보
        current_user: 현재 인증된 사용자 (의존성 주입)
        
    Returns:
        Dict: 수정된 스케줄 정보
    """
    config = get_config()
    schedule_file = os.path.join(config.schedules_dir, f"{schedule_id}.json")
    
    if not os.path.exists(schedule_file):
        raise HTTPException(status_code=404, detail="스케줄을 찾을 수 없습니다")
    
    # 파이프라인 존재 여부 확인
    if schedule.pipeline_id:
        pipeline_file = os.path.join(config.pipelines_dir, f"{schedule.pipeline_id}.json")
        if not os.path.exists(pipeline_file):
            raise HTTPException(status_code=404, detail="지정한 파이프라인을 찾을 수 없습니다")
    
    try:
        # 기존 스케줄 데이터 로드
        with open(schedule_file, 'r') as f:
            schedule_data = json.load(f)
        
        # 스케줄 정보 업데이트
        if schedule.name is not None:
            schedule_data["name"] = schedule.name
            schedule_data["description"] = schedule.name  # 호환성을 위해 두 필드 모두 업데이트
        
        if schedule.pipeline_id is not None:
            schedule_data["pipeline_id"] = schedule.pipeline_id
        
        if schedule.cron_expression is not None:
            schedule_data["cron_expression"] = schedule.cron_expression
            # 다음 실행 시간 재계산
            schedule_data["next_run"] = calculate_next_run(schedule.cron_expression)
        
        if schedule.enabled is not None:
            schedule_data["enabled"] = schedule.enabled
        
        if schedule.parameters is not None:
            schedule_data["parameters"] = schedule.parameters
            schedule_data["params"] = schedule.parameters  # 호환성을 위해 두 필드 모두 업데이트
        
        schedule_data["updated_at"] = datetime.now().isoformat()
        
        # 파일에 저장
        with open(schedule_file, 'w') as f:
            json.dump(schedule_data, f, indent=2)
        
        # 스케줄러 업데이트 시도
        if schedule_data["enabled"]:
            try:
                orchestrator = get_orchestrator()
                
                # 기존 스케줄 제거 후 재등록
                orchestrator.remove_schedule(schedule_id)
                
                # 활성화된 경우에만 재등록
                orchestrator.schedule_pipeline(
                    schedule_id=str(schedule_id),
                    pipeline_id=schedule_data["pipeline_id"],
                    cron_expression=schedule_data["cron_expression"],
                    parameters=schedule_data.get("parameters", {})
                )
                logger.info(f"스케줄러 업데이트 성공: {schedule_id}")
            except Exception as e:
                # 스케줄러 업데이트 실패 시에도 파일 업데이트는 유지
                logger.error(f"스케줄러 업데이트 실패: {str(e)}")
        else:
            # 비활성화된 경우 스케줄에서 제거
            try:
                orchestrator = get_orchestrator()
                orchestrator.remove_schedule(schedule_id)
                logger.info(f"스케줄러에서 제거 성공: {schedule_id}")
            except Exception as e:
                logger.error(f"스케줄러에서 제거 실패: {str(e)}")
        
        return schedule_data
    except Exception as e:
        logger.error(f"스케줄 파일 업데이트 오류: {str(e)}")
        raise HTTPException(status_code=500, detail="스케줄 업데이트 중 오류가 발생했습니다")

@router.delete("/{schedule_id}", status_code=204)
async def delete_schedule(
    schedule_id: str,
    current_user: User = Depends(get_current_active_user)
):
    """
    스케줄 삭제
    
    Arguments:
        schedule_id: 삭제할 스케줄 ID
        current_user: 현재 인증된 사용자 (의존성 주입)
        
    Returns:
        None: 204 No Content
    """
    config = get_config()
    schedule_file = os.path.join(config.schedules_dir, f"{schedule_id}.json")
    
    if not os.path.exists(schedule_file):
        raise HTTPException(status_code=404, detail="스케줄을 찾을 수 없습니다")
    
    # 스케줄러에서 제거 시도
    try:
        orchestrator = get_orchestrator()
        orchestrator.remove_schedule(schedule_id)
        logger.info(f"스케줄러에서 제거 성공: {schedule_id}")
    except Exception as e:
        # 스케줄러에서 제거 실패 시에도 파일 삭제는 진행
        logger.error(f"스케줄러에서 제거 실패: {str(e)}")
    
    try:
        # 스케줄 파일 삭제
        os.remove(schedule_file)
        return None
    except Exception as e:
        logger.error(f"스케줄 파일 삭제 오류: {str(e)}")
        raise HTTPException(status_code=500, detail="스케줄 삭제 중 오류가 발생했습니다")

@router.post("/{schedule_id}/run", status_code=202)
async def run_schedule(
    schedule_id: str,
    current_user: User = Depends(get_current_active_user)
):
    """
    스케줄 즉시 실행
    
    Arguments:
        schedule_id: 실행할 스케줄 ID
        current_user: 현재 인증된 사용자 (의존성 주입)
        
    Returns:
        dict: 실행 상태 정보
    """
    config = get_config()
    schedule_file = os.path.join(config.schedules_dir, f"{schedule_id}.json")
    
    if not os.path.exists(schedule_file):
        raise HTTPException(status_code=404, detail="스케줄을 찾을 수 없습니다")
    
    try:
        # 스케줄 파일 읽기
        with open(schedule_file, 'r') as f:
            schedule = json.load(f)
        
        # 파이프라인 존재 여부 확인
        pipeline_id = schedule.get("pipeline_id")
        pipeline_file = os.path.join(config.pipelines_dir, f"{pipeline_id}.json")
        
        if not os.path.exists(pipeline_file):
            raise HTTPException(status_code=404, detail="연결된 파이프라인을 찾을 수 없습니다")
        
        # 파이프라인 실행
        orchestrator = get_orchestrator()
        parameters = schedule.get("parameters", {}) or schedule.get("params", {})
        
        result = orchestrator.run_pipeline(
            pipeline_id=pipeline_id,
            config={},
            parameters=parameters,
            async_execution=True,
            trigger=f"manual_schedule_{schedule_id}"
        )
        
        # 실행 정보가 있는 경우 execution_id 추출
        execution_id = result.get("execution_id", str(uuid.uuid4()))
        
        # 실행 이력 생성
        execution_data = {
            "id": execution_id,
            "pipeline_id": pipeline_id,
            "status": "running",
            "trigger": f"schedule_manual_{schedule_id}",
            "schedule_id": schedule_id,
            "started_at": datetime.now().isoformat(),
            "logs": "스케줄에서 수동 실행 시작..."
        }
        
        # 실행 이력 저장
        execution_file = os.path.join(config.executions_dir, f"{execution_id}.json")
        with open(execution_file, 'w') as f:
            json.dump(execution_data, f, indent=2)
        
        return {
            "status": "success",
            "message": "스케줄이 실행되었습니다",
            "execution_id": execution_id,
            "pipeline_id": pipeline_id,
            "schedule_id": schedule_id
        }
    except Exception as e:
        logger.error(f"스케줄 실행 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"스케줄 실행 중 오류가 발생했습니다: {str(e)}") 
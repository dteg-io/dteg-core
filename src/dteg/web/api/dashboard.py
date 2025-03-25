"""
DTEG Web API - 대시보드

대시보드 데이터 엔드포인트
"""
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import logging
import os
import json
import glob

from fastapi import APIRouter, Depends

from dteg.web.api.auth import get_current_active_user
from dteg.web.api.models import User, MetricsSummary
from dteg.config import get_config

# 로거 설정
logger = logging.getLogger(__name__)

# 라우터 정의
router = APIRouter()

@router.get("/metrics", response_model=MetricsSummary)
async def get_metrics(
    current_user: User = Depends(get_current_active_user)
):
    """
    대시보드 메트릭 요약 정보 조회
    
    Arguments:
        current_user: 현재 인증된 사용자 (의존성 주입)
        
    Returns:
        MetricsSummary: 메트릭 요약 정보
    """
    config = get_config()
    executions_dir = os.path.join(config.storage_path, "executions")
    pipelines_dir = os.path.join(config.storage_path, "pipelines")
    schedules_dir = os.path.join(config.storage_path, "schedules")
    
    logger.info(f"설정 경로: storage_path={config.storage_path}")
    logger.info(f"실행 이력 경로: executions_dir={executions_dir}")
    logger.info(f"이 경로가 존재하는지 확인: {os.path.exists(executions_dir)}")
    
    # 실행 이력 경로의 상위 디렉토리 모든 파일 확인
    if os.path.exists(config.storage_path):
        all_files = os.listdir(config.storage_path)
        logger.info(f"스토리지 경로의 모든 파일/디렉토리: {all_files}")
        
        # executions 디렉토리가 있는지 확인
        if "executions" in all_files and os.path.isdir(os.path.join(config.storage_path, "executions")):
            execution_subfiles = os.listdir(os.path.join(config.storage_path, "executions"))
            logger.info(f"executions 디렉토리의 파일 수: {len(execution_subfiles)}")
    
    # 모든 가능한 경로 확인
    possible_paths = [
        executions_dir,
        os.path.join(os.getcwd(), "data/executions"),
        os.path.join(os.getcwd(), "executions"),
        os.path.join(os.path.dirname(os.getcwd()), "data/executions")
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            files = glob.glob(os.path.join(path, "*.json"))
            logger.info(f"가능한 경로 '{path}'에서 발견된 JSON 파일 수: {len(files)}")
    
    # 파이프라인 개수
    pipeline_files = glob.glob(os.path.join(pipelines_dir, "*.json"))
    total_pipelines = len(pipeline_files)
    logger.info(f"전체 파이프라인 수: {total_pipelines}")
    
    # 활성화된 스케줄 개수
    schedule_files = glob.glob(os.path.join(schedules_dir, "*.json"))
    active_schedules = 0
    for schedule_file in schedule_files:
        try:
            with open(schedule_file, 'r') as f:
                schedule = json.load(f)
                if schedule.get("enabled", False):
                    active_schedules += 1
        except Exception as e:
            logger.error(f"스케줄 파일 읽기 오류: {str(e)}")
    
    logger.info(f"활성화된 스케줄 수: {active_schedules}")
    
    # 실행 이력 파일 로드
    execution_files = glob.glob(os.path.join(executions_dir, "*.json"))
    total_executions = len(execution_files)
    logger.info(f"전체 실행 이력 수: {total_executions}")
    
    # 상태별 카운터 초기화
    completed_count = 0
    failed_count = 0
    running_count = 0
    cancelled_count = 0
    
    # 최근 24시간 실행 이력
    recent_executions = []
    one_day_ago = datetime.now() - timedelta(days=1)
    
    # 모든 실행 이력 파일 처리
    executions = []
    for execution_file in execution_files:
        try:
            with open(execution_file, 'r') as f:
                execution = json.load(f)
                executions.append(execution)
                
                # 시작 시간 파싱
                started_at = None
                if "started_at" in execution:
                    try:
                        started_at = datetime.fromisoformat(execution["started_at"].replace("Z", "+00:00"))
                    except Exception:
                        started_at = None
                
                # 상태별 카운트
                status = execution.get("status", "").lower()
                if status == "completed":
                    completed_count += 1
                elif status == "failed":
                    failed_count += 1
                elif status == "running":
                    running_count += 1
                elif status == "cancelled" or status == "canceled":
                    cancelled_count += 1
                
                # 최근 24시간 실행 이력 수집
                if started_at and started_at >= one_day_ago:
                    recent_executions.append(execution)
        except Exception as e:
            logger.error(f"실행 이력 파일 읽기 오류: {str(e)}")
    
    logger.info(f"상태별 실행 수 - 완료됨: {completed_count}, 실패: {failed_count}, 실행 중: {running_count}, 취소됨: {cancelled_count}")
    
    # 완료된 실행 목록
    completed_executions = [e for e in executions if e.get("status", "").lower() == "completed"]
    logger.info(f"완료된 실행 목록 개수: {len(completed_executions)}")
    
    if len(completed_executions) > 0:
        # 중복 파이프라인 ID 확인
        pipeline_ids = [e.get("pipeline_id", "") for e in completed_executions]
        unique_pipeline_ids = set(pipeline_ids)
        logger.info(f"완료된 실행의 파이프라인 ID 개수: {len(pipeline_ids)}, 중복 제거 후: {len(unique_pipeline_ids)}")
        
        # 임의의 샘플 10개 로깅
        sample_executions = completed_executions[:10]
        for i, execution in enumerate(sample_executions):
            logger.info(f"샘플 완료된 실행 {i+1}: ID={execution.get('id', 'unknown')}, 파이프라인ID={execution.get('pipeline_id', 'unknown')}, 시작시간={execution.get('started_at', 'unknown')}")
    
    # 최근 24시간 실행 성공률 계산
    recent_success_rate = 0
    if recent_executions:
        success_count = sum(1 for e in recent_executions if e.get("status", "").lower() == "completed")
        recent_success_rate = success_count / len(recent_executions) * 100
    
    pipeline_status = {
        "completed": completed_count,
        "failed": failed_count,
        "running": running_count,
        "cancelled": cancelled_count
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
    current_user: User = Depends(get_current_active_user)
):
    """
    최근 실행 이력 조회
    
    Arguments:
        limit: 조회할 최대 개수
        current_user: 현재 인증된 사용자 (의존성 주입)
        
    Returns:
        List[dict]: 최근 실행 이력 목록
    """
    config = get_config()
    executions_dir = os.path.join(config.storage_path, "executions")
    pipelines_dir = os.path.join(config.storage_path, "pipelines")
    
    # 모든 실행 이력 파일 로드
    execution_files = glob.glob(os.path.join(executions_dir, "*.json"))
    executions = []
    
    for execution_file in execution_files:
        try:
            with open(execution_file, 'r') as f:
                execution = json.load(f)
                
                # 시작 시간 추가
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
    
    # 시작 시간 기준으로 정렬하고 limit 적용
    executions.sort(key=lambda x: x["started_at_datetime"], reverse=True)
    recent_executions = executions[:limit]
    
    # 파이프라인 정보 로드
    pipelines = {}
    for execution in recent_executions:
        pipeline_id = execution.get("pipeline_id", "")
        if pipeline_id and pipeline_id not in pipelines:
            pipeline_file = os.path.join(pipelines_dir, f"{pipeline_id}.json")
            if os.path.exists(pipeline_file):
                try:
                    with open(pipeline_file, 'r') as f:
                        pipeline = json.load(f)
                        pipelines[pipeline_id] = pipeline.get("name", "알 수 없음")
                except Exception as e:
                    logger.error(f"파이프라인 파일 읽기 오류: {str(e)}")
                    pipelines[pipeline_id] = "알 수 없음"
            else:
                pipelines[pipeline_id] = "알 수 없음"
    
    # 응답 구성
    result = []
    for execution in recent_executions:
        pipeline_id = execution.get("pipeline_id", "")
        pipeline_name = pipelines.get(pipeline_id, "알 수 없음")
        
        result.append({
            "id": execution.get("id", ""),
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline_name,
            "status": execution.get("status", ""),
            "created_at": execution.get("started_at", ""),
            "completed_at": execution.get("ended_at", ""),
            "trigger": execution.get("trigger", "")
        })
    
    return result

@router.get("/execution_stats")
async def get_execution_stats(
    days: int = 7,
    current_user: User = Depends(get_current_active_user)
):
    """
    기간별 실행 통계 조회
    
    Arguments:
        days: 조회할 일수 (기본 7일)
        current_user: 현재 인증된 사용자 (의존성 주입)
        
    Returns:
        dict: 날짜별 실행 통계
    """
    config = get_config()
    executions_dir = os.path.join(config.storage_path, "executions")
    
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
    
    # 모든 실행 이력 파일 로드
    execution_files = glob.glob(os.path.join(executions_dir, "*.json"))
    
    for execution_file in execution_files:
        try:
            with open(execution_file, 'r') as f:
                execution = json.load(f)
                
                # 시작 시간 파싱
                if "started_at" in execution:
                    try:
                        started_at = datetime.fromisoformat(execution["started_at"].replace("Z", "+00:00"))
                        # 기간 내 실행만 처리
                        if started_at >= start_date:
                            date_str = started_at.date().isoformat()
                            if date_str in stats:
                                stats[date_str]["total"] += 1
                                
                                status = execution.get("status", "").lower()
                                if status == "completed":
                                    stats[date_str]["completed"] += 1
                                elif status == "failed":
                                    stats[date_str]["failed"] += 1
                                elif status == "cancelled" or status == "canceled":
                                    stats[date_str]["cancelled"] += 1
                    except Exception:
                        pass
        except Exception as e:
            logger.error(f"실행 이력 파일 읽기 오류: {str(e)}")
    
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
    current_user: User = Depends(get_current_active_user)
):
    """
    파이프라인별 실행 통계 조회
    
    Arguments:
        limit: 조회할 최대 파이프라인 수
        current_user: 현재 인증된 사용자 (의존성 주입)
        
    Returns:
        List[dict]: 파이프라인별 실행 통계
    """
    config = get_config()
    executions_dir = os.path.join(config.storage_path, "executions")
    pipelines_dir = os.path.join(config.storage_path, "pipelines")
    
    # 파이프라인 목록 로드
    pipeline_files = glob.glob(os.path.join(pipelines_dir, "*.json"))
    pipelines = {}
    
    for pipeline_file in pipeline_files:
        try:
            with open(pipeline_file, 'r') as f:
                pipeline = json.load(f)
                pipeline_id = pipeline.get("id", "")
                if pipeline_id:
                    pipelines[pipeline_id] = pipeline
        except Exception as e:
            logger.error(f"파이프라인 파일 읽기 오류: {str(e)}")
    
    # 실행 이력 로드
    execution_files = glob.glob(os.path.join(executions_dir, "*.json"))
    pipeline_executions = {}
    
    for execution_file in execution_files:
        try:
            with open(execution_file, 'r') as f:
                execution = json.load(f)
                pipeline_id = execution.get("pipeline_id", "")
                
                if pipeline_id:
                    if pipeline_id not in pipeline_executions:
                        pipeline_executions[pipeline_id] = []
                    
                    # 시작 시간 파싱
                    if "started_at" in execution:
                        try:
                            started_at = datetime.fromisoformat(execution["started_at"].replace("Z", "+00:00"))
                            execution["started_at_datetime"] = started_at
                        except Exception:
                            execution["started_at_datetime"] = datetime.min
                    else:
                        execution["started_at_datetime"] = datetime.min
                    
                    pipeline_executions[pipeline_id].append(execution)
        except Exception as e:
            logger.error(f"실행 이력 파일 읽기 오류: {str(e)}")
    
    # 파이프라인별 통계 계산
    pipeline_stats = []
    
    for pipeline_id, executions in pipeline_executions.items():
        if pipeline_id not in pipelines:
            continue
        
        pipeline = pipelines[pipeline_id]
        total = len(executions)
        completed = sum(1 for e in executions if e.get("status", "").lower() == "completed")
        failed = sum(1 for e in executions if e.get("status", "").lower() == "failed")
        
        success_rate = (completed / total * 100) if total > 0 else 0
        
        # 가장 최근 실행
        if executions:
            executions.sort(key=lambda x: x["started_at_datetime"], reverse=True)
            recent_execution = executions[0]
        else:
            continue
        
        pipeline_stats.append({
            "id": pipeline_id,
            "name": pipeline.get("name", "알 수 없음"),
            "total_executions": total,
            "success_rate": success_rate,
            "last_execution": {
                "id": recent_execution.get("id", ""),
                "status": recent_execution.get("status", ""),
                "created_at": recent_execution.get("started_at", ""),
                "completed_at": recent_execution.get("ended_at", "")
            }
        })
    
    # 총 실행 횟수 기준으로 정렬하고 limit 적용
    pipeline_stats.sort(key=lambda x: x["total_executions"], reverse=True)
    return pipeline_stats[:limit] 
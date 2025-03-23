"""
스케줄 명령어 모듈

dteg.cli.main에서 사용하는 스케줄 관련 명령어 구현
"""
import sys
import logging
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone

import click
from rich.console import Console
from rich.table import Table
from croniter import croniter

from dteg.orchestration import get_orchestrator
from dteg.core.config import PipelineConfig
from dteg.utils.rich_utils import spinner

console = Console()
logger = logging.getLogger(__name__)

# 웹 DB 연동을 위한 임포트
def sync_with_web_db(schedule_id=None, action="sync", schedule_data=None):
    """
    웹 DB와 스케줄 동기화
    
    Args:
        schedule_id: 스케줄 ID (특정 스케줄만 동기화할 경우)
        action: 수행할 작업 (sync, add, update, delete)
        schedule_data: 추가 또는 업데이트할 스케줄 데이터
        
    Returns:
        bool: 성공 여부
    """
    try:
        from dteg.web.database import SessionLocal
        from dteg.web.models.database_models import Schedule, Pipeline
        
        db = SessionLocal()
        try:
            if action == "sync":
                # 전체 동기화는 orchestrator에서 처리
                orchestrator = get_orchestrator()
                return orchestrator.sync_schedules_with_web_db()
                
            elif action == "add" and schedule_data:
                # 파이프라인 ID 확인
                pipeline_config = schedule_data.get("pipeline_config")
                
                # 파이프라인 설정 파일이 문자열이면 이를 경로로 간주
                if isinstance(pipeline_config, str) and (Path(pipeline_config).exists() or "/" in pipeline_config or "\\" in pipeline_config):
                    # 파이프라인 설정 파일에서 ID 추출
                    try:
                        config = PipelineConfig.from_yaml(pipeline_config)
                        pipeline_id = config.pipeline_id
                    except Exception as e:
                        logger.error(f"파이프라인 설정 로드 실패: {e}")
                        return False
                else:
                    # 직접 ID로 제공된 경우
                    pipeline_id = pipeline_config
                
                # 파이프라인 존재 여부 확인
                pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
                if not pipeline:
                    logger.error(f"파이프라인 {pipeline_id}가 DB에 존재하지 않습니다.")
                    return False
                
                # 새 스케줄 생성
                db_schedule = Schedule(
                    id=schedule_id,
                    pipeline_id=pipeline_id,
                    cron_expression=schedule_data.get("cron_expression"),
                    enabled=schedule_data.get("enabled", True),
                    description=schedule_data.get("name", f"CLI에서 생성된 스케줄 {schedule_id}")
                )
                
                # 다음 실행 시간 계산
                db_schedule.next_run = db_schedule.calculate_next_run()
                
                # DB에 추가
                db.add(db_schedule)
                db.commit()
                logger.info(f"웹 DB에 스케줄 {schedule_id} 추가됨")
                
            elif action == "update" and schedule_id and schedule_data:
                # 스케줄 존재 여부 확인
                db_schedule = db.query(Schedule).filter(Schedule.id == schedule_id).first()
                if not db_schedule:
                    logger.error(f"스케줄 {schedule_id}가 DB에 존재하지 않습니다.")
                    return False
                
                # 업데이트할 필드 설정
                if "cron_expression" in schedule_data:
                    db_schedule.cron_expression = schedule_data["cron_expression"]
                
                if "enabled" in schedule_data:
                    db_schedule.enabled = schedule_data["enabled"]
                
                if "name" in schedule_data:
                    db_schedule.description = schedule_data["name"]
                
                # 다음 실행 시간 재계산
                db_schedule.next_run = db_schedule.calculate_next_run()
                db_schedule.updated_at = datetime.now()
                
                # DB 업데이트
                db.commit()
                logger.info(f"웹 DB의 스케줄 {schedule_id} 업데이트됨")
                
            elif action == "delete" and schedule_id:
                # 스케줄 존재 여부 확인
                db_schedule = db.query(Schedule).filter(Schedule.id == schedule_id).first()
                if db_schedule:
                    # DB에서 삭제
                    db.delete(db_schedule)
                    db.commit()
                    logger.info(f"웹 DB에서 스케줄 {schedule_id} 삭제됨")
                else:
                    logger.warning(f"스케줄 {schedule_id}가 DB에 존재하지 않습니다.")
                    
            return True
        finally:
            db.close()
    except Exception as e:
        logger.error(f"웹 DB 동기화 중 오류 발생: {e}")
        return False

def add_schedule(pipeline_config_path: str, cron_expression: str, enabled: bool = True, schedule_id: Optional[str] = None, name: Optional[str] = None) -> Dict[str, Any]:
    """
    스케줄 추가
    
    Args:
        pipeline_config_path: 파이프라인 설정 파일 경로
        cron_expression: Cron 표현식
        enabled: 활성화 여부
        schedule_id: 스케줄 ID (None이면 자동 생성)
        name: 스케줄 이름 (None이면 자동 생성)
        
    Returns:
        Dict[str, Any]: 생성된 스케줄 정보
    """
    # 파이프라인 설정 파일 경로 확인
    pipeline_path = Path(pipeline_config_path)
    if not pipeline_path.exists():
        console.print(f"[bold red]✗[/] 파이프라인 설정 파일을 찾을 수 없습니다: {pipeline_config_path}")
        sys.exit(1)
    
    # Cron 표현식 검증
    if not croniter.is_valid(cron_expression):
        console.print(f"[bold red]✗[/] 유효하지 않은 Cron 표현식: {cron_expression}")
        sys.exit(1)
        
    # 스케줄 ID 생성 (지정되지 않은 경우)
    if schedule_id is None:
        schedule_id = str(uuid.uuid4())
    
    # 스케줄 이름 설정 (지정되지 않은 경우)
    if name is None:
        name = f"Schedule for {pipeline_path.stem}"
    
    # 스케줄러 객체 가져오기
    orchestrator = get_orchestrator()
    
    # 웹 DB 동기화
    schedule_data = {
        "pipeline_config": str(pipeline_path),
        "cron_expression": cron_expression,
        "enabled": enabled,
        "name": name
    }
    
    # 로컬 스케줄 등록
    try:
        with spinner("스케줄 등록 중..."):
            orchestrator.schedule_pipeline(
                schedule_id=schedule_id,
                pipeline_id=str(pipeline_path), 
                cron_expression=cron_expression
            )
            
            # 웹 DB에도 동기화
            sync_with_web_db(schedule_id, "add", schedule_data)
            
    except Exception as e:
        console.print(f"[bold red]✗[/] 스케줄 등록 실패: {str(e)}")
        sys.exit(1)
    
    return {
        "schedule_id": schedule_id,
        "pipeline_id": str(pipeline_path),
        "cron_expression": cron_expression,
        "enabled": enabled,
        "name": name
    }

def update_schedule(schedule_id: str, cron_expression: Optional[str] = None, enabled: Optional[bool] = None) -> bool:
    """
    스케줄 업데이트
    
    Args:
        schedule_id: 스케줄 ID
        cron_expression: 변경할 Cron 표현식 (None이면 변경 안함)
        enabled: 변경할 활성화 여부 (None이면 변경 안함)
        
    Returns:
        bool: 업데이트 성공 여부
    """
    # Cron 표현식 검증
    if cron_expression is not None and not croniter.is_valid(cron_expression):
        console.print(f"[bold red]✗[/] 유효하지 않은 Cron 표현식: {cron_expression}")
        sys.exit(1)
    
    # 스케줄러 객체 가져오기
    orchestrator = get_orchestrator()
    
    # 현재 스케줄 정보 조회
    current_schedules = orchestrator.get_all_pipelines()
    schedule = None
    
    for s in current_schedules:
        if s["schedule_id"] == schedule_id:
            schedule = s
            break
    
    if schedule is None:
        console.print(f"[bold red]✗[/] 스케줄 ID {schedule_id}를 찾을 수 없습니다.")
        sys.exit(1)
    
    # 업데이트할 값 설정
    update_data = {}
    
    if cron_expression is not None:
        update_data["cron_expression"] = cron_expression
    
    if enabled is not None:
        update_data["enabled"] = enabled
    
    # 업데이트할 내용이 없으면 종료
    if not update_data:
        console.print("[yellow]업데이트할 내용이 없습니다.[/]")
        return True
    
    # 스케줄 업데이트
    try:
        with spinner("스케줄 업데이트 중..."):
            # 현재 스케줄 제거
            orchestrator.remove_schedule(schedule_id)
            
            # 새 값으로 재등록
            new_cron = update_data.get("cron_expression", schedule["cron_expression"])
            new_enabled = update_data.get("enabled", schedule["enabled"])
            
            if new_enabled:
                orchestrator.schedule_pipeline(
                    schedule_id=schedule_id,
                    pipeline_id=schedule["pipeline_id"],
                    cron_expression=new_cron
                )
                
            # 웹 DB 동기화
            sync_with_web_db(schedule_id, "update", update_data)
    
    except Exception as e:
        console.print(f"[bold red]✗[/] 스케줄 업데이트 실패: {str(e)}")
        sys.exit(1)
        
    return True
    
def delete_schedule(schedule_id: str) -> bool:
    """
    스케줄 삭제
    
    Args:
        schedule_id: 스케줄 ID
        
    Returns:
        bool: 삭제 성공 여부
    """
    # 스케줄러 객체 가져오기
    orchestrator = get_orchestrator()
    
    # 스케줄 삭제
    try:
        with spinner("스케줄 삭제 중..."):
            result = orchestrator.remove_schedule(schedule_id)
            
            if result:
                # 웹 DB에서도 삭제
                sync_with_web_db(schedule_id, "delete")
                return True
            else:
                console.print(f"[bold red]✗[/] 스케줄 ID {schedule_id}를 찾을 수 없습니다.")
                return False
                
    except Exception as e:
        console.print(f"[bold red]✗[/] 스케줄 삭제 실패: {str(e)}")
        sys.exit(1)
        
def list_schedules() -> List[Dict[str, Any]]:
    """
    등록된 스케줄 목록 조회
    
    Returns:
        List[Dict[str, Any]]: 스케줄 목록
    """
    # 스케줄러 객체 가져오기
    orchestrator = get_orchestrator()
    
    # 스케줄 목록 조회
    try:
        # 웹 DB와 동기화
        sync_with_web_db()
        
        # 스케줄 목록 가져오기
        schedules = orchestrator.get_all_pipelines()
        return schedules
        
    except Exception as e:
        console.print(f"[bold red]✗[/] 스케줄 목록 조회 실패: {str(e)}")
        sys.exit(1) 
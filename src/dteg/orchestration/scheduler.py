"""
스케줄러 모듈

파이프라인의 스케줄링 및 실행 관리를 위한 클래스 구현
"""
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Callable, Union
import croniter
import uuid
from pathlib import Path
import os
import json

from dteg.core.pipeline import Pipeline
from dteg.core.config import PipelineConfig

logger = logging.getLogger(__name__)

class ScheduleConfig:
    """파이프라인 스케줄 설정 클래스"""
    
    def __init__(
        self,
        pipeline_config: Union[PipelineConfig, str, Path],
        cron_expression: str,
        enabled: bool = True,
        dependencies: List[str] = None,
        max_retries: int = 3,
        retry_delay: int = 300  # 5분
    ):
        """
        스케줄 설정 초기화
        
        Args:
            pipeline_config: 파이프라인 설정 객체 또는 설정 파일 경로
            cron_expression: Cron 표현식 (예: "0 0 * * *" - 매일 자정)
            enabled: 스케줄 활성화 여부
            dependencies: 이 파이프라인의 실행 전에 완료되어야 하는 파이프라인 ID 목록
            max_retries: 실패 시 최대 재시도 횟수
            retry_delay: 재시도 간 지연 시간(초)
        """
        self.id = str(uuid.uuid4())
        self.pipeline_config = pipeline_config
        self.cron_expression = cron_expression
        self.enabled = enabled
        self.dependencies = dependencies or []
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.next_run = self._get_next_run()
        
        # 유효성 검사
        if not croniter.croniter.is_valid(cron_expression):
            raise ValueError(f"유효하지 않은 Cron 표현식: {cron_expression}")
    
    def _get_next_run(self) -> datetime:
        """다음 실행 시간 계산"""
        cron = croniter.croniter(self.cron_expression, datetime.now())
        return cron.get_next(ret_type=datetime)
    
    def update_next_run(self):
        """다음 실행 시간 업데이트"""
        cron = croniter.croniter(self.cron_expression, datetime.now())
        self.next_run = cron.get_next(ret_type=datetime)

    def to_dict(self) -> Dict:
        """사전 형태로 변환"""
        pipeline_config_str = str(self.pipeline_config)
        if isinstance(self.pipeline_config, PipelineConfig):
            pipeline_config_str = self.pipeline_config.pipeline_id
            
        return {
            "id": self.id,
            "pipeline_config": pipeline_config_str,
            "cron_expression": self.cron_expression,
            "enabled": self.enabled,
            "dependencies": self.dependencies,
            "max_retries": self.max_retries,
            "retry_delay": self.retry_delay,
            "next_run": self.next_run.isoformat() if self.next_run else None
        }
    
    @classmethod
    def from_dict(cls, data: Dict, schedule_dir: Optional[Path] = None) -> 'ScheduleConfig':
        """사전에서 스케줄 설정 복원"""
        # pipeline_config 처리
        pipeline_config = data["pipeline_config"]
        
        # 경로인 경우 확인
        if isinstance(pipeline_config, str) and schedule_dir and "/" in pipeline_config:
            # 상대 경로로 처리할 수 있는지 확인
            pipeline_path = Path(pipeline_config)
            if not pipeline_path.exists() and schedule_dir:
                # 스케줄 디렉토리 기준 상대 경로 시도
                alt_path = schedule_dir.parent / pipeline_config
                if alt_path.exists():
                    pipeline_config = str(alt_path)
        
        instance = cls(
            pipeline_config=pipeline_config,
            cron_expression=data["cron_expression"],
            enabled=data["enabled"],
            dependencies=data["dependencies"],
            max_retries=data["max_retries"],
            retry_delay=data["retry_delay"]
        )
        
        # ID 복원
        instance.id = data["id"]
        
        # 다음 실행 시간 복원
        if data.get("next_run"):
            instance.next_run = datetime.fromisoformat(data["next_run"])
        else:
            instance.update_next_run()
            
        return instance


class ExecutionRecord:
    """파이프라인 실행 기록 클래스"""
    
    def __init__(self, schedule_id: str, pipeline_id: str):
        """
        실행 기록 초기화
        
        Args:
            schedule_id: 스케줄 ID
            pipeline_id: 파이프라인 ID
        """
        self.id = str(uuid.uuid4())
        self.schedule_id = schedule_id
        self.pipeline_id = pipeline_id
        self.start_time = datetime.now()
        self.end_time = None
        self.status = "RUNNING"  # RUNNING, SUCCESS, FAILED, RETRYING
        self.retry_count = 0
        self.error_message = None
        self.logs = []
    
    def complete(self, success: bool, error_message: Optional[str] = None):
        """
        실행 완료 처리
        
        Args:
            success: 성공 여부
            error_message: 오류 메시지 (실패 시)
        """
        self.end_time = datetime.now()
        self.status = "SUCCESS" if success else "FAILED"
        self.error_message = error_message
    
    def retry(self, error_message: str):
        """
        재시도 처리
        
        Args:
            error_message: 오류 메시지
        """
        self.retry_count += 1
        self.status = "RETRYING"
        self.error_message = error_message
    
    def to_dict(self) -> Dict:
        """사전 형태로 변환"""
        return {
            "id": self.id,
            "schedule_id": self.schedule_id,
            "pipeline_id": self.pipeline_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "status": self.status,
            "retry_count": self.retry_count,
            "error_message": self.error_message,
            "logs": self.logs
        }


class Scheduler:
    """파이프라인 스케줄러 클래스"""
    
    def __init__(self, 
                 history_dir: Optional[Union[str, Path]] = None,
                 schedule_dir: Optional[Union[str, Path]] = None,
                 on_execution_complete: Optional[Callable[[ExecutionRecord], None]] = None):
        """
        스케줄러 초기화
        
        Args:
            history_dir: 실행 이력을 저장할 디렉토리 (기본값: ~/.dteg/history)
            schedule_dir: 스케줄 설정을 저장할 디렉토리 (기본값: ~/.dteg/schedules)
            on_execution_complete: 실행 완료 시 호출될 콜백 함수
        """
        self.schedules: Dict[str, ScheduleConfig] = {}
        self.running_executions: Dict[str, ExecutionRecord] = {}
        self.completed_executions: List[ExecutionRecord] = []
        self.on_execution_complete = on_execution_complete
        
        # 이력 디렉토리 설정
        if history_dir is None:
            history_dir = Path.home() / ".dteg" / "history"
        self.history_dir = Path(history_dir)
        self.history_dir.mkdir(parents=True, exist_ok=True)
        
        # 스케줄 디렉토리 설정
        if schedule_dir is None:
            schedule_dir = Path.home() / ".dteg" / "schedules"
        self.schedule_dir = Path(schedule_dir)
        self.schedule_dir.mkdir(parents=True, exist_ok=True)
        
        # 이전 실행 이력 및 스케줄 로드
        self._load_history()
        self._load_schedules()
    
    def add_schedule(self, schedule_config: ScheduleConfig) -> str:
        """
        스케줄 추가
        
        Args:
            schedule_config: 스케줄 설정 객체
            
        Returns:
            추가된 스케줄의 ID
        """
        self.schedules[schedule_config.id] = schedule_config
        logger.info(f"스케줄 추가됨: {schedule_config.id} - 다음 실행: {schedule_config.next_run}")
        # 스케줄 저장
        self._save_schedules()
        return schedule_config.id
    
    def remove_schedule(self, schedule_id: str) -> bool:
        """
        스케줄 제거
        
        Args:
            schedule_id: 스케줄 ID
            
        Returns:
            제거 성공 여부
        """
        if schedule_id in self.schedules:
            del self.schedules[schedule_id]
            logger.info(f"스케줄 제거됨: {schedule_id}")
            # 스케줄 저장
            self._save_schedules()
            return True
        return False
    
    def get_schedule(self, schedule_id: str) -> Optional[ScheduleConfig]:
        """스케줄 ID로 스케줄 조회"""
        return self.schedules.get(schedule_id)
    
    def get_all_schedules(self) -> List[ScheduleConfig]:
        """모든 스케줄 조회"""
        return list(self.schedules.values())
    
    def update_schedule(self, schedule_id: str, **kwargs) -> bool:
        """
        스케줄 업데이트
        
        Args:
            schedule_id: 스케줄 ID
            **kwargs: 업데이트할 속성과 값
            
        Returns:
            업데이트 성공 여부
        """
        if schedule_id not in self.schedules:
            return False
        
        schedule = self.schedules[schedule_id]
        for key, value in kwargs.items():
            if hasattr(schedule, key):
                setattr(schedule, key, value)
        
        # Cron 표현식이 업데이트되었으면 다음 실행 시간 재계산
        if "cron_expression" in kwargs:
            schedule.update_next_run()
        
        logger.info(f"스케줄 업데이트됨: {schedule_id}")
        # 스케줄 저장
        self._save_schedules()
        return True
    
    def run_once(self):
        """
        실행 대기 중인 스케줄을 확인하고 실행
        """
        now = datetime.now()
        
        logger.debug(f"스케줄 확인 중... (현재 시간: {now.strftime('%Y-%m-%d %H:%M:%S')})")
        pending_schedule_count = 0
        executed_count = 0
        
        # 실행 대기 중인 스케줄 확인
        for schedule_id, schedule in self.schedules.items():
            if not schedule.enabled:
                logger.debug(f"스케줄 {schedule_id}는 비활성화 상태입니다")
                continue
            
            logger.debug(f"스케줄 {schedule_id} 다음 실행 시간: {schedule.next_run.strftime('%Y-%m-%d %H:%M:%S')}")
            
            if schedule.next_run <= now:
                pending_schedule_count += 1
                pipeline_id = getattr(schedule.pipeline_config, 'pipeline_id', str(schedule.pipeline_config))
                
                logger.info(f"🔔 실행 대기 중인 스케줄 발견: {schedule_id} (파이프라인: {pipeline_id})")
                
                # 의존성 확인
                if self._check_dependencies(schedule):
                    logger.info(f"▶️ 파이프라인 실행 시작: {schedule_id} → {pipeline_id}")
                    self._run_pipeline(schedule)
                    schedule.update_next_run()
                    executed_count += 1
                    logger.info(f"⏭️ 다음 실행 시간 업데이트: {schedule.next_run.strftime('%Y-%m-%d %H:%M:%S')}")
                    # 스케줄 저장 (다음 실행 시간 업데이트)
                    self._save_schedules()
                else:
                    logger.warning(f"⚠️ 스케줄 {schedule_id}의 의존성이 충족되지 않았습니다. 다음 기회에 재시도합니다.")
        
        # 실행 요약 메시지
        if pending_schedule_count > 0:
            logger.info(f"📊 스케줄 실행 요약: 대기 {pending_schedule_count}개 중 {executed_count}개 실행됨")
    
    def _check_dependencies(self, schedule: ScheduleConfig) -> bool:
        """
        스케줄의 의존성 충족 여부 확인
        
        Args:
            schedule: 스케줄 설정 객체
            
        Returns:
            의존성 충족 여부
        """
        if not schedule.dependencies:
            return True
            
        # 의존 파이프라인의 최근 실행 상태 확인
        for dep_id in schedule.dependencies:
            dep_successful = False
            
            # 완료된 실행 목록에서 확인
            for exec_record in reversed(self.completed_executions):
                if exec_record.pipeline_id == dep_id and exec_record.status == "SUCCESS":
                    dep_successful = True
                    break
            
            if not dep_successful:
                return False
                
        return True
    
    def _run_pipeline(self, schedule: ScheduleConfig):
        """
        파이프라인 실행
        
        Args:
            schedule: 스케줄 설정 객체
        """
        # 파이프라인 설정 로드
        if isinstance(schedule.pipeline_config, (str, Path)):
            config = PipelineConfig.from_yaml(schedule.pipeline_config)
        else:
            config = schedule.pipeline_config
        
        # 실행 기록 생성
        pipeline_id = getattr(config, "pipeline_id", str(schedule.pipeline_config))
        execution = ExecutionRecord(schedule.id, pipeline_id)
        self.running_executions[execution.id] = execution
        
        try:
            # 명확하고 눈에 띄는 실행 메시지 출력
            logger.info("=" * 60)
            logger.info(f"🚀 파이프라인 실행 시작: {pipeline_id} (스케줄: {schedule.id})")
            logger.info(f"⏰ 실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 60)
            
            # 파이프라인 실행
            pipeline = Pipeline(config)
            pipeline.run()
            
            # 실행 완료 처리
            execution.complete(success=True)
            
            # 실행 완료 메시지 출력
            logger.info("=" * 60)
            logger.info(f"✅ 파이프라인 실행 완료: {pipeline_id} (스케줄: {schedule.id})")
            logger.info(f"⏰ 완료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 60)
            
        except Exception as e:
            error_msg = str(e)
            
            # 실행 실패 메시지 출력
            logger.error("=" * 60)
            logger.error(f"❌ 파이프라인 실행 실패: {pipeline_id} (스케줄: {schedule.id})")
            logger.error(f"🔥 오류: {error_msg}")
            logger.error("=" * 60)
            
            # 재시도 여부 결정
            if execution.retry_count < schedule.max_retries:
                execution.retry(error_msg)
                logger.warning(f"⚠️ 파이프라인 재시도 예정: {pipeline_id} (재시도: {execution.retry_count}/{schedule.max_retries})")
                # TODO: 실제 재시도 로직 구현 (별도 스레드나 지연 실행 등)
            else:
                execution.complete(success=False, error_message=error_msg)
                logger.error(f"❌ 최대 재시도 횟수 초과. 파이프라인 실행 종료: {pipeline_id}")
        
        # 실행 기록 처리
        del self.running_executions[execution.id]
        self.completed_executions.append(execution)
        self._save_execution_record(execution)
        
        # 콜백 호출
        if self.on_execution_complete:
            self.on_execution_complete(execution)
    
    def run_scheduler(self, interval: int = 60):
        """
        스케줄러 실행 루프
        
        Args:
            interval: 스케줄 확인 간격(초)
        """
        logger.info("스케줄러 시작됨")
        try:
            while True:
                self.run_once()
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("스케줄러 중지됨")
    
    def _save_execution_record(self, execution: ExecutionRecord):
        """실행 기록 저장"""
        record_path = self.history_dir / f"{execution.id}.json"
        with open(record_path, 'w', encoding='utf-8') as f:
            json.dump(execution.to_dict(), f, indent=2, ensure_ascii=False)
    
    def _save_schedules(self):
        """모든 스케줄 설정 저장"""
        # 스케줄 정보 저장
        schedules_data = {}
        for schedule_id, schedule in self.schedules.items():
            schedules_data[schedule_id] = schedule.to_dict()
        
        # 스케줄 JSON 파일 저장
        schedules_path = self.schedule_dir / "schedules.json"
        with open(schedules_path, 'w', encoding='utf-8') as f:
            json.dump(schedules_data, f, indent=2, ensure_ascii=False)
        
        logger.debug(f"스케줄 정보가 {schedules_path}에 저장되었습니다.")
    
    def _load_schedules(self):
        """저장된 스케줄 정보 로드"""
        schedules_path = self.schedule_dir / "schedules.json"
        if not schedules_path.exists():
            logger.info("저장된 스케줄 정보가 없습니다.")
            return
            
        try:
            with open(schedules_path, 'r', encoding='utf-8') as f:
                schedules_data = json.load(f)
                
            for schedule_id, schedule_data in schedules_data.items():
                schedule = ScheduleConfig.from_dict(schedule_data, self.schedule_dir)
                self.schedules[schedule_id] = schedule
                
            logger.info(f"{len(self.schedules)}개의 스케줄 정보를 로드했습니다.")
        except Exception as e:
            logger.error(f"스케줄 정보 로드 실패: {e}")
    
    def _load_history(self):
        """저장된 실행 이력 로드"""
        if not self.history_dir.exists():
            return
            
        for file_path in self.history_dir.glob("*.json"):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                # 실행 기록 복원 (간소화 버전)
                record = ExecutionRecord(data["schedule_id"], data["pipeline_id"])
                record.id = data["id"]
                record.start_time = datetime.fromisoformat(data["start_time"])
                if data["end_time"]:
                    record.end_time = datetime.fromisoformat(data["end_time"])
                record.status = data["status"]
                record.retry_count = data["retry_count"]
                record.error_message = data["error_message"]
                record.logs = data.get("logs", [])
                
                self.completed_executions.append(record)
            except Exception as e:
                logger.error(f"실행 이력 로드 실패: {file_path} - {e}")
        
        logger.info(f"{len(self.completed_executions)}개의 실행 이력을 로드했습니다.") 
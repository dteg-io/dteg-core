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

    def get_next_run_time(self) -> Optional[datetime]:
        """다음 실행 시간 반환
        
        Returns:
            Optional[datetime]: 다음 실행 시간 (비활성화된 경우 None)
        """
        if not self.enabled:
            return None
            
        cron = croniter.croniter(self.cron_expression, datetime.now())
        return cron.get_next(ret_type=datetime)

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
        for schedule_id, schedule in list(self.schedules.items()):
            try:
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
                        try:
                            self._run_pipeline(schedule)
                        except Exception as e:
                            logger.error(f"⚠️ 파이프라인 실행 실패: {schedule_id} → {pipeline_id}: {str(e)}")
                            # 실패해도 다음 실행 시간 업데이트
                            
                        # 다음 실행 시간 업데이트는 실행 성공 여부와 관계없이 수행
                        schedule.update_next_run()
                        executed_count += 1
                        logger.info(f"⏭️ 다음 실행 시간 업데이트: {schedule.next_run.strftime('%Y-%m-%d %H:%M:%S')}")
                        # 스케줄 저장 (다음 실행 시간 업데이트)
                        self._save_schedules()
                    else:
                        logger.warning(f"⚠️ 스케줄 {schedule_id}의 의존성이 충족되지 않았습니다. 다음 기회에 재시도합니다.")
            except Exception as e:
                logger.error(f"⚠️ 스케줄 {schedule_id} 처리 중 오류 발생: {str(e)}")
                continue
        
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
    
    def _run_pipeline(self, schedule):
        """
        파이프라인 실행
        
        Args:
            schedule: 스케줄 설정 객체
        """
        # 파이프라인 ID 또는 설정 파일 경로
        pipeline_config = schedule.pipeline_config
        schedule_id = schedule.id
        
        # 실행 기록 생성
        try:
            # 파이프라인 ID 추출
            if isinstance(pipeline_config, (str, Path)):
                # 파일 경로인 경우 설정 파일에서 ID 추출 시도
                try:
                    if Path(pipeline_config).exists():
                        # 파일이 존재하는 경우 설정 로드
                        config = PipelineConfig.from_yaml(pipeline_config)
                        pipeline_id = config.pipeline_id
                    else:
                        # 파일이 존재하지 않으면 문자열을 ID로 간주
                        pipeline_id = str(pipeline_config)
                except Exception as e:
                    logger.warning(f"파이프라인 설정 로드 실패, ID로 처리합니다: {e}")
                    pipeline_id = str(pipeline_config)
            else:
                # PipelineConfig 객체인 경우
                pipeline_id = pipeline_config.pipeline_id
                
            record = ExecutionRecord(schedule_id, pipeline_id)
            self.running_executions[record.id] = record
            
            # 시작 로그 추가
            record.logs.append(f"[{datetime.now().isoformat()}] 🚀 파이프라인 실행 시작: {pipeline_id}")
            record.logs.append(f"[{datetime.now().isoformat()}] ⏰ 실행 시간: {record.start_time.isoformat()}")
            
            # 로그 핸들러 설정 - 파이프라인 실행 중 로그를 수집하기 위한 핸들러
            log_collector = []
            
            class LogHandler(logging.Handler):
                def emit(self, record):
                    log_entry = self.format(record)
                    log_collector.append(log_entry)
            
            # 로그 핸들러 등록
            log_handler = LogHandler()
            log_handler.setFormatter(logging.Formatter('%(message)s'))
            logging.getLogger('dteg').addHandler(log_handler)
            
            # 여기서 실제 파이프라인 실행
            success = False
            try:
                if Path(pipeline_config).exists():
                    # 실제 파일이 존재하는 경우에만 파일로 로드
                    pipeline = Pipeline.from_config(pipeline_config)
                    pipeline.run()
                    success = True
                else:
                    # ID만 있는 경우 (웹 UI에서 등록된 경우) 
                    # 로그만 남기고 실제 실행하지 않음 - 여기서 DB에서 파이프라인 정보를 가져와서 실행하는 코드가 필요
                    logger.info(f"파이프라인 ID {pipeline_id}를 사용한 실행 (웹 UI 등록 스케줄)")
                    record.logs.append(f"[{datetime.now().isoformat()}] 📋 웹 UI에서 등록된 파이프라인 ID: {pipeline_id}")
                    
                    # 웹 DB에서 파이프라인 정보 조회 시도
                    db_pipeline = self._get_pipeline_from_db(pipeline_id)
                    if db_pipeline:
                        # 파이프라인 정보가 DB에 있으면 실행
                        record.logs.append(f"[{datetime.now().isoformat()}] 💾 데이터베이스에서 파이프라인 정보 로드됨")
                        
                        try:
                            # DB에서 가져온 설정에 'pipeline' 필드가 있는지 확인
                            config_data = db_pipeline.config
                            
                            record.logs.append(f"[{datetime.now().isoformat()}] 📦 파이프라인 설정 로드됨")
                            
                            # 새로운 설정 형식 지원
                            if 'pipeline' in config_data:
                                # 이전 형식: {'pipeline': {...}} 구조
                                # 필요한 설정 추출
                                pipeline_config = config_data['pipeline']
                                # Pipeline 객체 생성
                                pipeline = Pipeline(pipeline_config)
                                record.logs.append(f"[{datetime.now().isoformat()}] 🔄 이전 형식의 파이프라인 설정 사용")
                            else:
                                # 새로운 형식: 평면적 구조
                                # Pipeline 객체 생성
                                pipeline = Pipeline(config_data)
                                record.logs.append(f"[{datetime.now().isoformat()}] 🔄 새로운 형식의 파이프라인 설정 사용")
                            
                            record.logs.append(f"[{datetime.now().isoformat()}] 🏃 파이프라인 실행 중...")
                            pipeline.run()
                            record.logs.append(f"[{datetime.now().isoformat()}] 🏁 파이프라인 실행 완료")
                            success = True
                        except Exception as e:
                            error_msg = f"파이프라인 실행 실패: {str(e)}"
                            record.logs.append(f"[{datetime.now().isoformat()}] ❌ {error_msg}")
                            logger.error(error_msg)
                            raise
                        
                    else:
                        record.logs.append(f"[{datetime.now().isoformat()}] ❌ 데이터베이스에서 파이프라인 정보를 찾을 수 없음")
                        raise ValueError(f"파이프라인 ID {pipeline_id}에 대한 정보를 찾을 수 없습니다")
                
                # 로그 수집기에서 로그 가져와서 실행 기록에 추가
                for log_entry in log_collector:
                    record.logs.append(f"[{datetime.now().isoformat()}] {log_entry}")
                
                # 성공 로그 추가
                record.logs.append(f"[{datetime.now().isoformat()}] ✅ 파이프라인 실행 완료")
                record.complete(success=True)
                
            except Exception as e:
                error_message = f"파이프라인 실행 중 오류 발생: {str(e)}"
                logger.error(error_message)
                
                # 오류 로그 추가
                record.logs.append(f"[{datetime.now().isoformat()}] ❌ {error_message}")
                record.complete(success=False, error_message=error_message)
                
                # 예외 전파하지 않고 오류 처리
                success = False
            finally:
                # 로그 핸들러 제거
                logging.getLogger('dteg').removeHandler(log_handler)
            
            # 종료 로그 추가
            record.logs.append(f"[{datetime.now().isoformat()}] ⏰ 완료 시간: {record.end_time.isoformat() if record.end_time else datetime.now().isoformat()}")
            
            # 실행 기록 저장
            self._save_execution_record(record)
            
            # 콜백 호출
            if self.on_execution_complete:
                self.on_execution_complete(record)
                
            # 스케줄 업데이트
            if success:
                # 다음 실행 시간 업데이트
                next_run = schedule.get_next_run_time()
                schedule.last_run_time = datetime.now()
                schedule.last_run_status = "SUCCESS"
                schedule.next_run_time = next_run
                
                logger.info(f"⏭️ 다음 실행 시간 업데이트: {next_run.strftime('%Y-%m-%d %H:%M:%S') if next_run else '없음'}")
            else:
                # 실패 시에도 다음 실행 시간 업데이트 (실패해도 계속 실행)
                next_run = schedule.get_next_run_time()
                schedule.last_run_time = datetime.now()
                schedule.last_run_status = "FAILED" 
                schedule.next_run_time = next_run
                
                logger.info(f"⏭️ 다음 실행 시간 업데이트 (실패 후): {next_run.strftime('%Y-%m-%d %H:%M:%S') if next_run else '없음'}")
            
            # 스케줄 저장
            self._save_schedule(schedule)
            
            return success
        except Exception as e:
            logger.error(f"파이프라인 실행 중 예외 발생: {str(e)}")
            return False
    
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
        """
        실행 기록 저장
        
        Args:
            execution: 실행 기록 객체
        """
        try:
            # 웹 UI용 DB에 저장 시도
            from dteg.web.database import SessionLocal
            from dteg.web.models.database_models import Execution as DBExecution
            
            db = SessionLocal()
            try:
                # 기존 실행 기록 확인
                db_execution = db.query(DBExecution).filter(DBExecution.id == execution.id).first()
                
                if db_execution:
                    # 기존 실행 기록 업데이트
                    db_execution.status = execution.status
                    db_execution.ended_at = execution.end_time
                    db_execution.error_message = execution.error_message
                    db_execution.logs = '\n'.join(execution.logs) if execution.logs else None
                else:
                    # 새 실행 기록 생성
                    db_execution = DBExecution(
                        id=execution.id,
                        pipeline_id=execution.pipeline_id,
                        schedule_id=execution.schedule_id,
                        status=execution.status,
                        started_at=execution.start_time,
                        ended_at=execution.end_time,
                        error_message=execution.error_message,
                        logs='\n'.join(execution.logs) if execution.logs else None
                    )
                    db.add(db_execution)
                
                db.commit()
                logger.debug(f"실행 기록 {execution.id}가 데이터베이스에 저장되었습니다.")
            except Exception as e:
                db.rollback()
                logger.error(f"실행 기록 저장 중 데이터베이스 오류: {str(e)}")
            finally:
                db.close()
        except ImportError:
            logger.debug("웹 UI 데이터베이스 모듈이 로드되지 않았습니다. 실행 기록이 로컬에만 저장됩니다.")
        except Exception as e:
            logger.error(f"실행 기록 저장 중 오류: {str(e)}")
            
    def _get_pipeline_from_db(self, pipeline_id: str):
        """
        데이터베이스에서 파이프라인 정보를 조회
        
        Args:
            pipeline_id: 파이프라인 ID
            
        Returns:
            파이프라인 객체 또는 None
        """
        try:
            from dteg.web.database import SessionLocal
            from dteg.web.models.database_models import Pipeline as DBPipeline
            
            db = SessionLocal()
            try:
                # DB에서 파이프라인 정보 조회
                db_pipeline = db.query(DBPipeline).filter(DBPipeline.id == pipeline_id).first()
                return db_pipeline
            except Exception as e:
                logger.error(f"DB에서 파이프라인 정보 조회 중 오류: {str(e)}")
                return None
            finally:
                db.close()
        except ImportError:
            logger.warning("웹 UI 데이터베이스 모듈이 로드되지 않았습니다.")
            return None
        except Exception as e:
            logger.error(f"파이프라인 정보 조회 중 오류: {str(e)}")
            return None
    
    def _save_history(self):
        """모든 실행 이력 저장"""
        for execution in self.completed_executions:
            self._save_execution_record(execution)
        logger.debug(f"실행 이력이 {self.history_dir}에 저장되었습니다.")
    
    def _save_schedules(self):
        """모든 스케줄 설정 저장"""
        # 디렉토리 생성
        os.makedirs(self.schedule_dir, exist_ok=True)
        
        # 개별 스케줄 JSON 파일 저장
        for schedule in self.schedules.values():
            # 스케줄 ID 기반 파일명
            filename = f"{schedule.id}.json"
            filepath = self.schedule_dir / filename
            
            # 사전으로 변환 후 JSON 저장
            schedule_dict = schedule.to_dict()
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(schedule_dict, f, indent=2, ensure_ascii=False)
                
        logger.debug(f"{len(self.schedules)}개의 스케줄 정보가 저장되었습니다.")
        
    def _save_schedule(self, schedule: ScheduleConfig):
        """특정 스케줄 설정 저장
        
        Args:
            schedule: 저장할 스케줄 설정 객체
        """
        try:
            # 디렉토리 생성
            os.makedirs(self.schedule_dir, exist_ok=True)
            
            # 스케줄 ID 기반 파일명
            filename = f"{schedule.id}.json"
            filepath = self.schedule_dir / filename
            
            # 사전으로 변환 후 JSON 저장
            schedule_dict = schedule.to_dict()
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(schedule_dict, f, indent=2, ensure_ascii=False)
                
            # 메모리상의 스케줄 갱신
            self.schedules[schedule.id] = schedule
            
            logger.debug(f"스케줄 {schedule.id}가 저장되었습니다.")
            
            # SQLite 데이터베이스 업데이트
            try:
                from dteg.web.database import SessionLocal
                from dteg.web.models.database_models import Schedule as DBSchedule
                
                db = SessionLocal()
                try:
                    db_schedule = db.query(DBSchedule).filter(DBSchedule.id == schedule.id).first()
                    if db_schedule:
                        # 다음 실행 시간 업데이트
                        if hasattr(schedule, 'next_run_time') and schedule.next_run_time:
                            db_schedule.next_run = schedule.next_run_time
                        elif hasattr(schedule, 'next_run') and schedule.next_run:
                            db_schedule.next_run = schedule.next_run
                            
                        logger.debug(f"DB 스케줄 {schedule.id}의 다음 실행 시간 업데이트됨")
                    
                    # 변경 사항 커밋
                    db.commit()
                    logger.debug(f"SQLite 데이터베이스에 스케줄 {schedule.id} 정보가 업데이트되었습니다.")
                except Exception as e:
                    db.rollback()
                    logger.error(f"SQLite 데이터베이스 업데이트 실패: {str(e)}")
                finally:
                    db.close()
            except ImportError:
                logger.debug("웹 UI 데이터베이스 모듈이 로드되지 않았습니다.")
            except Exception as e:
                logger.error(f"데이터베이스 연결 실패: {str(e)}")
        except Exception as e:
            logger.error(f"스케줄 저장 중 오류 발생: {str(e)}")
            
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
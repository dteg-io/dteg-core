"""
오케스트레이션 관리자 모듈

스케줄러와 작업 큐를 통합하여 파이프라인 실행을 관리하는 모듈
"""
import logging
import threading
import time
from typing import Dict, List, Optional, Union, Any, Callable
from pathlib import Path
import uuid
import sys
import traceback

from dteg.orchestration.scheduler import Scheduler, ScheduleConfig, ExecutionRecord
from dteg.orchestration.worker import CeleryTaskManager, CeleryTaskQueue
from dteg.core.config import PipelineConfig

logger = logging.getLogger(__name__)

class OrchestratorError(Exception):
    """오케스트레이션 관련 오류 클래스"""
    pass

class Orchestrator:
    """파이프라인 오케스트레이션 관리자 클래스"""
    
    def __init__(self, 
                 history_dir: Optional[Union[str, Path]] = None,
                 result_dir: Optional[Union[str, Path]] = None,
                 schedule_dir: Optional[Union[str, Path]] = None,
                 broker_url: Optional[str] = None,
                 result_backend: Optional[str] = None,
                 use_celery: bool = True,
                 on_execution_complete: Optional[Callable[[Dict[str, Any]], None]] = None):
        """
        오케스트레이션 관리자 초기화
        
        Args:
            history_dir: 실행 이력을 저장할 디렉토리
            result_dir: 결과를 저장할 디렉토리
            schedule_dir: 스케줄 설정을 저장할 디렉토리
            broker_url: Celery broker URL
            result_backend: Celery result backend
            use_celery: Celery 작업 큐 사용 여부
            on_execution_complete: 실행 완료 시 호출될 콜백 함수
        """
        self.use_celery = use_celery
        self.on_execution_complete = on_execution_complete
        
        # 콜백 래퍼 함수 정의
        def execution_callback(record: ExecutionRecord):
            """스케줄러 실행 완료 콜백"""
            if self.on_execution_complete:
                # 외부 콜백 호출
                self.on_execution_complete({
                    "execution_id": record.id,
                    "pipeline_id": record.pipeline_id,
                    "status": record.status,
                    "start_time": record.start_time.isoformat(),
                    "end_time": record.end_time.isoformat() if record.end_time else None,
                    "error_message": record.error_message
                })
        
        # 스케줄러 초기화
        self.scheduler = Scheduler(
            history_dir=history_dir,
            schedule_dir=schedule_dir,
            on_execution_complete=execution_callback
        )
        
        # Celery 사용 시 작업 관리자 초기화
        if use_celery:
            self.task_manager = CeleryTaskManager(result_dir=result_dir, broker_url=broker_url, result_backend=result_backend)
        else:
            self.task_manager = None
        
        # 스케줄러 스레드
        self.scheduler_thread = None
        self.scheduler_running = False
    
    def add_pipeline(self, 
                     pipeline_config: Union[PipelineConfig, str, Path],
                     cron_expression: Optional[str] = None,
                     dependencies: List[str] = None,
                     enabled: bool = True,
                     max_retries: int = 3,
                     retry_delay: int = 300) -> str:
        """
        파이프라인 추가
        
        Args:
            pipeline_config: 파이프라인 설정 객체 또는 설정 파일 경로
            cron_expression: Cron 표현식 (예: "0 0 * * *" - 매일 자정)
            dependencies: 이 파이프라인의 실행 전에 완료되어야 하는 파이프라인 ID 목록
            enabled: 스케줄 활성화 여부
            max_retries: 실패 시 최대 재시도 횟수
            retry_delay: 재시도 간 지연 시간(초)
            
        Returns:
            스케줄 ID
        """
        # 스케줄 설정 생성 및 추가
        if cron_expression:
            # 스케줄링된 파이프라인
            schedule_config = ScheduleConfig(
                pipeline_config=pipeline_config,
                cron_expression=cron_expression,
                dependencies=dependencies,
                enabled=enabled,
                max_retries=max_retries,
                retry_delay=retry_delay
            )
            return self.scheduler.add_schedule(schedule_config)
        else:
            # 스케줄 없이 파이프라인 ID만 반환
            if isinstance(pipeline_config, (str, Path)):
                config = PipelineConfig.from_yaml(pipeline_config)
                return config.pipeline_id
            else:
                return pipeline_config.pipeline_id
    
    def run_pipeline(self, 
                     pipeline_id: str,
                     execution_id: Optional[str] = None,
                     async_execution: bool = True,
                     queue: str = "default") -> Dict[str, Any]:
        """
        파이프라인 실행
        
        Args:
            pipeline_id: 파이프라인 ID 또는 스케줄 ID
            execution_id: 실행 ID (선택적)
            async_execution: 비동기 실행 여부
            queue: 작업 큐 이름 (Celery 사용 시)
            
        Returns:
            실행 결과 정보
        """
        if execution_id is None:
            execution_id = str(uuid.uuid4())
            
        # 스케줄 확인
        schedule = self.scheduler.get_schedule(pipeline_id)
        
        if schedule:
            # 스케줄이 있는 경우, 스케줄의 파이프라인 설정 사용
            pipeline_config = schedule.pipeline_config
        else:
            # 스케줄이 없는 경우, 파이프라인 ID로 가정
            pipeline_id = pipeline_id
            # 여기서는 파이프라인 ID만 있고 설정이 없으므로, 
            # 실제로는 파이프라인 설정을 조회하는 코드가 필요함
            # 지금은 예시를 위해 생략
            raise OrchestratorError(f"파이프라인 ID {pipeline_id}에 대한 설정을 찾을 수 없습니다.")
        
        # 실행 방식 결정
        if self.use_celery and async_execution:
            # Celery를 사용한 비동기 실행
            task_id = self.task_manager.run_pipeline(
                pipeline_config=pipeline_config,
                execution_id=execution_id,
                queue=queue
            )
            
            return {
                "execution_id": execution_id,
                "task_id": task_id,
                "status": "submitted",
                "pipeline_id": pipeline_id
            }
        else:
            # 동기 실행 (스케줄러의 실행 로직 사용)
            if schedule:
                # 스케줄러를 통한 실행
                self.scheduler._run_pipeline(schedule)
                
                # 가장 최근 실행 기록 조회
                for record in reversed(self.scheduler.completed_executions):
                    if record.pipeline_id == pipeline_id:
                        return {
                            "execution_id": record.id,
                            "status": record.status,
                            "pipeline_id": pipeline_id,
                            "start_time": record.start_time.isoformat(),
                            "end_time": record.end_time.isoformat() if record.end_time else None,
                            "error_message": record.error_message
                        }
            
            # 실행 기록을 찾지 못한 경우
            return {
                "execution_id": execution_id,
                "status": "unknown",
                "pipeline_id": pipeline_id,
                "error_message": "실행 결과를 찾을 수 없습니다."
            }
    
    def get_pipeline_status(self, 
                          execution_id: Optional[str] = None,
                          task_id: Optional[str] = None) -> Dict[str, Any]:
        """
        파이프라인 실행 상태 조회
        
        Args:
            execution_id: 실행 ID (선택적)
            task_id: 작업 ID (Celery 사용 시, 선택적)
            
        Returns:
            실행 상태 정보
        """
        if task_id and self.use_celery:
            # Celery 작업 상태 조회
            return self.task_manager.get_result(task_id)
            
        elif execution_id:
            # 실행 ID로 스케줄러 이력에서 조회
            for record in self.scheduler.completed_executions:
                if record.id == execution_id:
                    return {
                        "execution_id": record.id,
                        "status": record.status,
                        "pipeline_id": record.pipeline_id,
                        "start_time": record.start_time.isoformat(),
                        "end_time": record.end_time.isoformat() if record.end_time else None,
                        "error_message": record.error_message
                    }
            
            # 실행 중인 작업에서 조회
            for exec_id, record in self.scheduler.running_executions.items():
                if exec_id == execution_id:
                    return {
                        "execution_id": record.id,
                        "status": "RUNNING",
                        "pipeline_id": record.pipeline_id,
                        "start_time": record.start_time.isoformat()
                    }
        
        # 상태를 찾지 못한 경우
        return {
            "status": "unknown",
            "execution_id": execution_id,
            "task_id": task_id,
            "error_message": "실행 상태를 찾을 수 없습니다."
        }
    
    def start_scheduler(self, interval: int = 60, no_immediate_run: bool = True):
        """
        스케줄러 시작
        
        Args:
            interval: 스케줄 확인 간격(초)
            no_immediate_run: True이면 스케줄러 시작 시 즉시 실행하지 않고 다음 간격까지 대기
        """
        if self.scheduler_running:
            logger.warning("스케줄러가 이미 실행 중입니다.")
            return
            
        self.scheduler_running = True
        
        # 스케줄러 스레드 생성 및 시작
        def scheduler_loop():
            logger.info("스케줄러 스레드 시작됨")
            
            # 출력 버퍼 플러시 설정
            sys.stdout.flush()
            
            # 스케줄러가 running 상태인 동안 계속 실행
            while self.scheduler_running:
                try:
                    # no_immediate_run이 True이고 첫 번째 실행인 경우 대기
                    if no_immediate_run and 'first_run' not in locals():
                        first_run = False
                        logger.info(f"즉시 실행 모드가 비활성화되었습니다. {interval}초 후 첫 번째 실행이 시작됩니다.")
                        time.sleep(interval)
                        continue
                    
                    # 스케줄 실행 (오류 처리는 run_once 내부에서 이미 처리)
                    self.scheduler.run_once()
                    
                    # 로그 출력 후 표준 출력 버퍼 강제 플러시
                    sys.stdout.flush()
                    
                    # 다음 확인 전 대기
                    time.sleep(interval)
                except Exception as e:
                    # 예외 발생 시 스택 트레이스 출력하고 계속 실행
                    logger.error(f"스케줄러 실행 중 오류 발생: {str(e)}")
                    logger.error(traceback.format_exc())
                    # 오류 발생 시에도 계속 실행
                    logger.info("스케줄러가 오류에서 회복을 시도합니다.")
                    # 짧은 시간만 대기 후 재시도
                    time.sleep(5)
                    
            logger.info("스케줄러 스레드 종료됨")
        
        self.scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
        self.scheduler_thread.start()
        
        if no_immediate_run:
            logger.info(f"스케줄러가 시작되었습니다 (간격: {interval}초, 즉시 실행 없음)")
        else:
            logger.info(f"스케줄러가 시작되었습니다 (간격: {interval}초)")
    
    def stop_scheduler(self):
        """스케줄러 중지"""
        if not self.scheduler_running:
            logger.warning("스케줄러가 실행 중이 아닙니다.")
            return
            
        logger.info("스케줄러 중지 중...")
        self.scheduler_running = False
        
        # 스레드 종료 대기
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=10)
            if self.scheduler_thread.is_alive():
                logger.warning("스케줄러 스레드가 10초 동안 종료되지 않았습니다.")
            else:
                logger.info("스케줄러가 정상적으로 중지되었습니다.")
    
    def get_all_pipelines(self) -> List[Dict[str, Any]]:
        """
        모든 파이프라인 정보 조회
        
        Returns:
            파이프라인 정보 목록
        """
        pipelines = []
        
        # 스케줄이 있는 파이프라인 조회
        for schedule in self.scheduler.get_all_schedules():
            # 파이프라인 ID 추출
            if isinstance(schedule.pipeline_config, (str, Path)):
                try:
                    config = PipelineConfig.from_yaml(schedule.pipeline_config)
                    pipeline_id = config.pipeline_id
                except Exception:
                    pipeline_id = str(schedule.pipeline_config)
            else:
                pipeline_id = schedule.pipeline_config.pipeline_id
                
            pipelines.append({
                "schedule_id": schedule.id,
                "pipeline_id": pipeline_id,
                "cron_expression": schedule.cron_expression,
                "next_run": schedule.next_run.isoformat(),
                "enabled": schedule.enabled,
                "dependencies": schedule.dependencies,
                "max_retries": schedule.max_retries
            })
        
        return pipelines
    
    def update_pipeline(self, 
                      schedule_id: str, 
                      enabled: Optional[bool] = None,
                      cron_expression: Optional[str] = None,
                      dependencies: Optional[List[str]] = None,
                      max_retries: Optional[int] = None) -> bool:
        """
        파이프라인 스케줄 업데이트
        
        Args:
            schedule_id: 스케줄 ID
            enabled: 활성화 여부
            cron_expression: Cron 표현식
            dependencies: 의존성 목록
            max_retries: 최대 재시도 횟수
            
        Returns:
            업데이트 성공 여부
        """
        update_args = {}
        
        if enabled is not None:
            update_args["enabled"] = enabled
        if cron_expression is not None:
            update_args["cron_expression"] = cron_expression
        if dependencies is not None:
            update_args["dependencies"] = dependencies
        if max_retries is not None:
            update_args["max_retries"] = max_retries
            
        if not update_args:
            logger.warning("업데이트할 속성이 지정되지 않았습니다.")
            return False
            
        return self.scheduler.update_schedule(schedule_id, **update_args)
    
    def remove_pipeline(self, schedule_id: str) -> bool:
        """
        파이프라인 제거
        
        Args:
            schedule_id: 스케줄 ID
            
        Returns:
            제거 성공 여부
        """
        return self.scheduler.remove_schedule(schedule_id)
    
    def cancel_execution(self, 
                        execution_id: Optional[str] = None,
                        task_id: Optional[str] = None) -> bool:
        """
        실행 취소
        
        Args:
            execution_id: 실행 ID (선택적)
            task_id: 작업 ID (Celery 사용 시, 선택적)
            
        Returns:
            취소 성공 여부
        """
        if task_id and self.use_celery:
            # Celery 작업 취소
            return self.task_manager.revoke_task(task_id, terminate=True)
    
    def add_pipeline_dependency(self, schedule_id: str, dependency_id: str) -> bool:
        """
        파이프라인 의존성 추가
        
        Args:
            schedule_id: 스케줄 ID
            dependency_id: 의존 대상 스케줄 ID
            
        Returns:
            추가 성공 여부
        """
        # 스케줄 확인
        schedule = self.scheduler.get_schedule(schedule_id)
        if not schedule:
            logger.warning(f"스케줄 ID {schedule_id}를 찾을 수 없습니다.")
            return False
        
        # 의존 대상 스케줄 확인
        dependency_schedule = self.scheduler.get_schedule(dependency_id)
        if not dependency_schedule:
            logger.warning(f"의존 대상 스케줄 ID {dependency_id}를 찾을 수 없습니다.")
            return False
        
        # 이미 의존성이 있으면 추가하지 않음
        if schedule.dependencies and dependency_id in schedule.dependencies:
            logger.info(f"스케줄 {schedule_id}는 이미 {dependency_id}에 의존하고 있습니다.")
            return False
            
        # 의존성 목록에 추가
        dependencies = list(schedule.dependencies) if schedule.dependencies else []
        dependencies.append(dependency_id)
        
        # 스케줄 업데이트
        return self.scheduler.update_schedule(schedule_id, dependencies=dependencies)
    
    def remove_pipeline_dependency(self, schedule_id: str, dependency_id: str) -> bool:
        """
        파이프라인 의존성 제거
        
        Args:
            schedule_id: 스케줄 ID
            dependency_id: 제거할 의존 대상 스케줄 ID
            
        Returns:
            제거 성공 여부
        """
        # 스케줄 확인
        schedule = self.scheduler.get_schedule(schedule_id)
        if not schedule:
            logger.warning(f"스케줄 ID {schedule_id}를 찾을 수 없습니다.")
            return False
        
        # 의존성이 없으면 제거할 수 없음
        if not schedule.dependencies or dependency_id not in schedule.dependencies:
            logger.warning(f"스케줄 {schedule_id}는 {dependency_id}에 의존하고 있지 않습니다.")
            return False
            
        # 의존성 목록에서 제거
        dependencies = list(schedule.dependencies)
        dependencies.remove(dependency_id)
        
        # 스케줄 업데이트
        return self.scheduler.update_schedule(schedule_id, dependencies=dependencies)
    
    def get_pipeline_dependencies(self, schedule_id: str) -> List[str]:
        """
        파이프라인 의존성 조회
        
        Args:
            schedule_id: 스케줄 ID
            
        Returns:
            의존성 목록
        """
        # 스케줄 확인
        schedule = self.scheduler.get_schedule(schedule_id)
        if not schedule:
            raise OrchestratorError(f"스케줄 ID {schedule_id}를 찾을 수 없습니다.")
            
        # 의존성 목록 반환
        return schedule.dependencies or [] 
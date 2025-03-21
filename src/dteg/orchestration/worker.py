"""
Celery 작업 큐 모듈

파이프라인 작업 분산 처리를 위한 Celery 기반 작업 큐 구현
"""
import logging
import os
from datetime import datetime
from typing import Dict, List, Any, Optional, Union, Callable
from pathlib import Path
import json
import time

from celery import Celery
from celery.exceptions import MaxRetriesExceededError
from celery.signals import task_failure, task_success

# Redis 모듈을 선택적으로 가져오기 (설치되어 있으면)
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

from dteg.core.pipeline import Pipeline
from dteg.core.config import PipelineConfig

logger = logging.getLogger(__name__)

# 환경 변수에서 Celery 브로커 URL 읽기, 없으면 기본값 사용
CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
CELERY_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

def get_broker_url() -> str:
    """Celery 브로커 URL 반환"""
    return CELERY_BROKER_URL

def get_result_backend() -> str:
    """Celery 결과 백엔드 URL 반환"""
    return CELERY_RESULT_BACKEND

# Celery 앱 생성
celery_app = Celery(
    "dteg",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND
)

# 기본 설정
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Seoul",  # 시간대 설정
    enable_utc=True,
    worker_prefetch_multiplier=1,  # 작업 동시 처리 설정
    task_acks_late=True,  # 작업 완료 후 승인 (실패 시 재처리 가능)
)

# 오류 발생 시 알림 처리
@task_failure.connect
def handle_task_failure(sender=None, task_id=None, exception=None, **kwargs):
    """작업 실패 시 처리 로직"""
    logger.error(f"작업 {task_id} 실패: {exception}")
    # 여기에 알림 로직 추가 (이메일, Slack 등)

@task_success.connect
def handle_task_success(sender=None, result=None, **kwargs):
    """작업 성공 시 처리 로직"""
    logger.info(f"작업 성공 완료: {result}")


@celery_app.task(bind=True, max_retries=3, default_retry_delay=300)
def pipeline_task(self, pipeline_config: Dict[str, Any], execution_id: Optional[str] = None):
    """
    파이프라인 실행 작업
    
    Args:
        pipeline_config: 파이프라인 설정 (사전 형태)
        execution_id: 실행 ID (선택적)
    
    Returns:
        실행 결과 정보
    """
    try:
        logger.info(f"파이프라인 실행 시작 (Task ID: {self.request.id})")
        
        # 파이프라인 실행
        pipeline = Pipeline(pipeline_config)
        pipeline.run()
        
        # 결과 반환
        return {
            "execution_id": execution_id,
            "success": True,
            "error": None
        }
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"파이프라인 실행 오류: {error_msg}")
        
        # 실패 결과 반환
        return {
            "execution_id": execution_id,
            "success": False,
            "error": f"Error: {error_msg}"
        }

def setup_celery(broker_url: str = None, result_backend: str = None) -> Celery:
    """
    Celery 앱 설정
    
    Args:
        broker_url: Celery 브로커 URL
        result_backend: Celery 결과 백엔드 URL
        
    Returns:
        설정된 Celery 앱
    """
    app = Celery(
        "dteg_worker",
        broker=broker_url or CELERY_BROKER_URL,
        backend=result_backend or CELERY_RESULT_BACKEND
    )
    
    # 기본 설정
    app.conf.update(
        task_serializer="pickle",
        accept_content=["pickle"],
        result_serializer="pickle"
    )
    
    return app

@celery_app.task(bind=True)
def run_pipeline_from_file(self, config_path: Union[str, Path], execution_id: Optional[str] = None):
    """
    파일에서 로드한 설정으로 파이프라인 실행
    
    Args:
        config_path: 파이프라인 설정 파일 경로
        execution_id: 실행 ID (선택적)
        
    Returns:
        실행 결과 정보
    """
    try:
        # 설정 파일 로드
        config = PipelineConfig.from_yaml(config_path)
        
        # 설정을 dict로 변환하여 기본 작업으로 전달
        config_dict = config.dict()
        return pipeline_task(config_dict, execution_id)
        
    except Exception as e:
        logger.error(f"파이프라인 설정 로드 실패: {e}")
        return {
            "task_id": self.request.id,
            "execution_id": execution_id,
            "status": "failed",
            "error": f"설정 로드 실패: {str(e)}"
        }

class CeleryTaskQueue:
    """Celery 작업 큐 클래스"""
    
    def __init__(self, broker_url: str = None, result_backend: str = None, on_task_complete: Optional[Callable] = None):
        """
        작업 큐 초기화
        
        Args:
            broker_url: Celery 브로커 URL
            result_backend: Celery 결과 백엔드 URL
            on_task_complete: 작업 완료 시 호출할 콜백 함수
        """
        self.broker_url = broker_url or CELERY_BROKER_URL
        self.result_backend = result_backend or CELERY_RESULT_BACKEND
        self.app = setup_celery(self.broker_url, self.result_backend)
        self.on_task_complete = on_task_complete
        self.celery_app = self.app  # 테스트 호환성을 위해 추가
    
    def run_pipeline(self, 
                     pipeline_config: Union[PipelineConfig, Dict[str, Any]],
                     execution_id: Optional[str] = None) -> str:
        """
        파이프라인 작업 제출
        
        Args:
            pipeline_config: 파이프라인 설정 (객체 또는 사전)
            execution_id: 실행 ID (선택적)
            
        Returns:
            작업 ID
        """
        if isinstance(pipeline_config, PipelineConfig):
            config_dict = pipeline_config.dict()
        else:
            config_dict = pipeline_config
                
        task = pipeline_task.delay(config_dict, execution_id)
        logger.info(f"파이프라인 작업 제출됨 (파이프라인 ID: {config_dict.get('pipeline_id', 'unknown')}, 작업 ID: {task.id})")
        
        return task.id
    
    def get_task_status(self, task_id: str) -> str:
        """
        작업 상태 조회
        
        Args:
            task_id: 작업 ID
            
        Returns:
            작업 상태 문자열
        """
        async_result = self.app.AsyncResult(task_id)
        return async_result.state
    
    def cancel_task(self, task_id: str, terminate: bool = True) -> bool:
        """
        작업 취소
        
        Args:
            task_id: 작업 ID
            terminate: 실행 중인 작업 강제 종료 여부
            
        Returns:
            취소 처리 성공 여부
        """
        try:
            self.app.control.revoke(task_id, terminate=terminate)
            logger.info(f"작업 취소됨: {task_id}")
            return True
        except Exception as e:
            logger.error(f"작업 취소 실패: {task_id} - {e}")
            return False

    @celery_app.task(bind=True)
    def run_pipeline_from_file(self, config_path: Union[str, Path], execution_id: Optional[str] = None):
        """
        파일에서 로드한 설정으로 파이프라인 실행
        
        Args:
            config_path: 파이프라인 설정 파일 경로
            execution_id: 실행 ID (선택적)
            
        Returns:
            실행 결과 정보
        """
        try:
            # 설정 파일 로드
            config = PipelineConfig.from_yaml(config_path)
            
            # 설정을 dict로 변환하여 기본 작업으로 전달
            config_dict = config.dict()
            return pipeline_task(config_dict, execution_id)
            
        except Exception as e:
            logger.error(f"파이프라인 설정 로드 실패: {e}")
            return {
                "task_id": self.request.id,
                "execution_id": execution_id,
                "status": "failed",
                "error": f"설정 로드 실패: {str(e)}"
            }

class CeleryTaskManager:
    """Celery 작업 관리자 클래스"""
    
    def __init__(self, result_dir: Optional[Union[str, Path]] = None, broker_url: Optional[str] = None, result_backend: Optional[str] = None):
        """
        작업 관리자 초기화
        
        Args:
            result_dir: 결과 저장 디렉토리 (기본값: ~/.dteg/results)
            broker_url: Celery 브로커 URL
            result_backend: Celery 결과 백엔드 URL
        """
        # 결과 저장 디렉토리 설정
        if result_dir is None:
            result_dir = Path.home() / ".dteg" / "results"
        self.result_dir = Path(result_dir)
        self.result_dir.mkdir(parents=True, exist_ok=True)
        
        # Celery 브로커 및 결과 백엔드 URL 설정
        self.broker_url = broker_url or CELERY_BROKER_URL
        self.result_backend = result_backend or CELERY_RESULT_BACKEND
        
        # Celery 큐 설정
        self.task_queue = CeleryTaskQueue(
            broker_url=self.broker_url,
            result_backend=self.result_backend
        )
    
    def run_pipeline(self, 
                     pipeline_config: Union[PipelineConfig, Dict[str, Any], str, Path],
                     execution_id: Optional[str] = None,
                     queue: str = "default",
                     countdown: int = 0,
                     eta: Optional[datetime] = None) -> str:
        """
        파이프라인 작업 제출
        
        Args:
            pipeline_config: 파이프라인 설정 (객체, 사전, 또는 파일 경로)
            execution_id: 실행 ID (선택적)
            queue: 사용할 Celery 큐 이름
            countdown: 지연 실행 시간(초)
            eta: 예약 실행 시간
            
        Returns:
            작업 ID
        """
        # 커스텀 Celery 큐를 통해 작업 제출
        if isinstance(pipeline_config, (str, Path)):
            # 파일 경로인 경우 파일에서 로드하는 작업 사용
            pipeline_id = str(pipeline_config)
        else:
            # 설정 객체나 사전인 경우
            if isinstance(pipeline_config, PipelineConfig):
                pipeline_id = pipeline_config.pipeline_id
            else:
                pipeline_id = pipeline_config.get("pipeline_id", "unknown")
                
        # task_queue를 사용하여 작업 제출
        task_id = self.task_queue.run_pipeline(
            pipeline_config=pipeline_config, 
            execution_id=execution_id
        )
        logger.info(f"파이프라인 작업 제출됨 (파이프라인 ID: {pipeline_id}, 작업 ID: {task_id})")
        
        return task_id
    
    def get_result(self, task_id: str, wait: bool = False, timeout: int = 10) -> Dict[str, Any]:
        """
        작업 결과 조회
        
        Args:
            task_id: 작업 ID
            wait: 결과를 기다릴지 여부
            timeout: 대기 시간(초)
            
        Returns:
            작업 결과 정보
        """
        # 설정한 task_queue로 AsyncResult 객체 생성
        async_result = self.task_queue.app.AsyncResult(task_id)
        
        if wait and not async_result.ready():
            try:
                # 결과 대기
                async_result.get(timeout=timeout)
            except Exception as e:
                logger.error(f"작업 결과 대기 중 오류: {e}")
        
        if async_result.successful():
            return {
                "task_id": task_id,
                "status": "success",
                "result": async_result.result
            }
        elif async_result.failed():
            return {
                "task_id": task_id,
                "status": "failed",
                "error": str(async_result.result)
            }
        else:
            return {
                "task_id": task_id,
                "status": async_result.status
            }
    
    def get_active_tasks(self) -> List[str]:
        """
        현재 실행 중인 작업 목록 조회
        
        Returns:
            실행 중인 작업 ID 목록
        """
        try:
            # Celery Inspect를 사용하여 현재 활성 작업 조회
            inspector = self.task_queue.app.control.inspect()
            active_tasks = inspector.active()
            
            if not active_tasks:
                return []
                
            # 모든 워커의 활성 작업 통합
            result = []
            for worker_name, tasks in active_tasks.items():
                worker_tasks = [task['id'] for task in tasks]
                result.extend(worker_tasks)
                
            return result
        except Exception as e:
            # Redis 연결 오류 특별 처리
            if REDIS_AVAILABLE and isinstance(e, redis.exceptions.ConnectionError):
                logger.debug("Redis 연결 실패: 실행 중인 작업 없음으로 간주")
            else:
                logger.debug(f"활성 작업 조회 중 오류: {e}")
            return []
    
    def revoke_task(self, task_id: str, terminate: bool = False) -> bool:
        """
        작업 취소
        
        Args:
            task_id: 작업 ID
            terminate: 실행 중인 작업 강제 종료 여부
            
        Returns:
            취소 처리 성공 여부
        """
        try:
            self.task_queue.app.control.revoke(task_id, terminate=terminate)
            logger.info(f"작업 취소됨: {task_id}")
            return True
        except Exception as e:
            logger.error(f"작업 취소 실패: {task_id} - {e}")
            return False
    
    def save_result(self, task_id: str, result: Dict[str, Any]):
        """
        작업 결과를 파일로 저장
        
        Args:
            task_id: 작업 ID
            result: 저장할 결과 데이터
        """
        result_file = self.result_dir / f"{task_id}.json"
        try:
            with open(result_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            logger.debug(f"작업 결과 저장됨: {result_file}")
        except Exception as e:
            logger.error(f"작업 결과 저장 실패: {e}") 
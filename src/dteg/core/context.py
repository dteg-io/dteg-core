"""
파이프라인 실행 컨텍스트 모듈
"""
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import pandas as pd


class ExecutionStatus(Enum):
    """파이프라인 실행 상태"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


@dataclass
class MetricsTracker:
    """성능 및 데이터 관련 지표 추적"""
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    rows_processed: int = 0
    bytes_processed: int = 0
    error_count: int = 0
    warning_count: int = 0
    
    def start(self) -> None:
        """측정 시작"""
        self.start_time = time.time()
    
    def stop(self) -> None:
        """측정 종료"""
        self.end_time = time.time()
    
    def get_execution_time(self) -> Optional[float]:
        """실행 시간(초) 계산
        
        Returns:
            실행 시간(초) 또는 None (측정 미완료 시)
        """
        if self.start_time is None or self.end_time is None:
            return None
        return self.end_time - self.start_time
    
    def update_row_count(self, count: int) -> None:
        """처리된 행 수 갱신
        
        Args:
            count: 처리된 행 수
        """
        self.rows_processed += count
    
    def update_byte_count(self, count: int) -> None:
        """처리된 바이트 수 갱신
        
        Args:
            count: 처리된 바이트 수
        """
        self.bytes_processed += count
    
    def increment_error(self) -> None:
        """오류 카운터 증가"""
        self.error_count += 1
    
    def increment_warning(self) -> None:
        """경고 카운터 증가"""
        self.warning_count += 1
    
    def to_dict(self) -> Dict[str, Any]:
        """지표를 딕셔너리로 변환
        
        Returns:
            지표 정보 딕셔너리
        """
        execution_time = self.get_execution_time()
        return {
            "execution_time_seconds": execution_time,
            "rows_processed": self.rows_processed,
            "bytes_processed": self.bytes_processed,
            "error_count": self.error_count,
            "warning_count": self.warning_count
        }


@dataclass
class PipelineContext:
    """파이프라인 실행 컨텍스트"""
    pipeline_name: str
    run_id: str = field(default_factory=lambda: datetime.now().strftime("%Y%m%d-%H%M%S"))
    config: Dict[str, Any] = field(default_factory=dict)
    status: ExecutionStatus = ExecutionStatus.PENDING
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    metrics: MetricsTracker = field(default_factory=MetricsTracker)
    variables: Dict[str, Any] = field(default_factory=dict)
    artifacts: Dict[str, Any] = field(default_factory=dict)
    logs: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def start(self) -> None:
        """파이프라인 실행 시작"""
        self.start_time = datetime.now()
        self.status = ExecutionStatus.RUNNING
        self.metrics.start()
        
    def complete(self, success: bool = True) -> None:
        """파이프라인 실행 완료
        
        Args:
            success: 성공 여부
        """
        self.end_time = datetime.now()
        self.status = ExecutionStatus.SUCCEEDED if success else ExecutionStatus.FAILED
        self.metrics.stop()
    
    def fail(self, error: Optional[Exception] = None) -> None:
        """파이프라인 실행 실패 처리
        
        Args:
            error: 발생한 예외 객체
        """
        self.end_time = datetime.now()
        self.status = ExecutionStatus.FAILED
        self.metrics.stop()
        
        if error:
            self.metadata["error"] = str(error)
            self.metrics.increment_error()
    
    def cancel(self) -> None:
        """파이프라인 실행 취소"""
        self.end_time = datetime.now()
        self.status = ExecutionStatus.CANCELED
        self.metrics.stop()
    
    def set_variable(self, name: str, value: Any) -> None:
        """변수 설정
        
        Args:
            name: 변수 이름
            value: 변수 값
        """
        self.variables[name] = value
    
    def get_variable(self, name: str, default: Any = None) -> Any:
        """변수 조회
        
        Args:
            name: 변수 이름
            default: 변수가 없을 경우 기본값
            
        Returns:
            변수 값 또는 기본값
        """
        return self.variables.get(name, default)
    
    def add_artifact(self, name: str, artifact: Any) -> None:
        """아티팩트 추가
        
        Args:
            name: 아티팩트 이름
            artifact: 아티팩트 객체
        """
        self.artifacts[name] = artifact
    
    def log_event(self, event_type: str, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        """이벤트 로깅
        
        Args:
            event_type: 이벤트 유형
            message: 이벤트 메시지
            details: 추가 세부 정보
        """
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "type": event_type,
            "message": message
        }
        
        if details:
            log_entry["details"] = details
            
        self.logs.append(log_entry)
    
    def update_metrics(self, data: Optional[pd.DataFrame] = None, **kwargs) -> None:
        """지표 업데이트
        
        Args:
            data: 데이터프레임(바이트 및 행 수 자동 계산)
            **kwargs: 직접 지정할 지표 값
        """
        if data is not None:
            # 행 수 업데이트
            self.metrics.update_row_count(len(data))
            
            # 대략적인 메모리 사용량 추정 (바이트)
            try:
                memory_usage = data.memory_usage(deep=True).sum()
                self.metrics.update_byte_count(memory_usage)
            except:
                pass
        
        # 직접 지정된 지표 업데이트
        for key, value in kwargs.items():
            if key == "rows":
                self.metrics.update_row_count(value)
            elif key == "bytes":
                self.metrics.update_byte_count(value)
            elif key == "error":
                if value:
                    self.metrics.increment_error()
            elif key == "warning":
                if value:
                    self.metrics.increment_warning()
    
    def to_dict(self) -> Dict[str, Any]:
        """컨텍스트 정보를 딕셔너리로 변환
        
        Returns:
            컨텍스트 정보 딕셔너리
        """
        return {
            "pipeline_name": self.pipeline_name,
            "run_id": self.run_id,
            "status": self.status.value,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "metrics": self.metrics.to_dict(),
            "logs_count": len(self.logs),
            "artifacts_count": len(self.artifacts),
            "variables_count": len(self.variables),
            "metadata": self.metadata
        } 
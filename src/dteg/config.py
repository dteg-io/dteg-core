"""
DTEG 설정 모듈
"""
import os
from pathlib import Path
from typing import Optional, Dict, Any

# 싱글톤 설정 인스턴스
_config_instance = None

class DtegConfig:
    """DTEG 설정 클래스"""
    
    def __init__(self, storage_path: Optional[str] = None):
        """
        DTEG 설정 초기화
        
        Args:
            storage_path: 파일 저장 경로 (기본값: DTEG_STORAGE_PATH 환경 변수 또는 ~/.dteg)
        """
        # 홈 디렉토리 가져오기
        home_dir = str(Path.home())
        
        # 기본 설정 - 홈 디렉토리의 .dteg 폴더 사용
        self.storage_path = storage_path or os.environ.get("DTEG_STORAGE_PATH", os.path.join(home_dir, ".dteg"))
        
        # 필요한 디렉토리 생성
        self._create_directories()
    
    def _create_directories(self):
        """필요한 디렉토리 생성"""
        # 메인 저장소 디렉토리
        os.makedirs(self.storage_path, exist_ok=True)
        
        # 서브 디렉토리
        for subdir in ["pipelines", "executions", "schedules", "logs"]:
            os.makedirs(os.path.join(self.storage_path, subdir), exist_ok=True)
    
    @property
    def pipelines_dir(self) -> str:
        """파이프라인 디렉토리 경로"""
        return os.path.join(self.storage_path, "pipelines")
    
    @property
    def executions_dir(self) -> str:
        """실행 이력 디렉토리 경로"""
        return os.path.join(self.storage_path, "executions")
    
    @property
    def schedules_dir(self) -> str:
        """스케줄 디렉토리 경로"""
        return os.path.join(self.storage_path, "schedules")
    
    @property
    def logs_dir(self) -> str:
        """로그 디렉토리 경로"""
        return os.path.join(self.storage_path, "logs")

def get_config(storage_path: Optional[str] = None) -> DtegConfig:
    """
    글로벌 DTEG 설정 인스턴스 반환
    
    Args:
        storage_path: 파일 저장 경로 (기본값: DTEG_STORAGE_PATH 환경 변수 또는 ~/.dteg)
    
    Returns:
        DtegConfig: DTEG 설정 인스턴스
    """
    global _config_instance
    if _config_instance is None:
        _config_instance = DtegConfig(storage_path)
    return _config_instance 
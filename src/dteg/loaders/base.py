"""
데이터 적재를 위한 기본 인터페이스
"""
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional

import pandas as pd


class IfExists(str, Enum):
    """기존 대상이 있을 때 처리 방법"""
    FAIL = "fail"  # 이미 존재하면 오류 발생
    REPLACE = "replace"  # 기존 데이터 삭제 후 새 데이터 적재
    APPEND = "append"  # 기존 데이터에 새 데이터 추가
    TRUNCATE = "truncate"  # 기존 데이터 삭제 후 새 데이터 적재 (테이블 구조 유지)


class Loader(ABC):
    """데이터를 대상 시스템에 적재하기 위한 추상 기본 클래스

    모든 Loader 구현체는 이 클래스를 상속해야 합니다.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Args:
            config: Loader 설정
        """
        self.config = config
        self._validate_config()
        self._setup()

    @abstractmethod
    def _validate_config(self) -> None:
        """설정 유효성 검사

        Raises:
            ValueError: 필수 설정이 누락되었거나 잘못된 경우
        """
        pass

    @abstractmethod
    def _setup(self) -> None:
        """대상 연결 설정"""
        pass

    @abstractmethod
    def load(self, data: pd.DataFrame) -> int:
        """데이터 적재 실행

        Args:
            data: 적재할 데이터

        Returns:
            적재된 행 수

        Raises:
            RuntimeError: 데이터 적재 중 오류 발생
        """
        pass

    def create_if_not_exists(self, data: pd.DataFrame) -> bool:
        """데이터 구조가 존재하지 않는 경우 생성

        Args:
            data: 적재할 데이터의 샘플(스키마 추론용)

        Returns:
            새로 생성되었으면 True, 이미 존재했으면 False

        Raises:
            RuntimeError: 구조 생성 중 오류 발생
        """
        # 기본 구현은 아무 작업도 수행하지 않음
        # 하위 클래스에서 필요에 따라 구현
        return False

    def get_current_schema(self) -> List[Dict[str, Any]]:
        """대상의 현재 스키마를 반환

        Returns:
            스키마 정보 (열 이름, 타입 등)
        """
        # 기본 구현은 빈 리스트 반환
        # 하위 클래스에서 필요에 따라 구현
        return []

    def close(self) -> None:
        """대상 연결 종료"""
        # 기본적으로 아무 작업도 수행하지 않음
        # 필요한 경우 하위 클래스에서 재정의
        pass

    def __enter__(self) -> 'Loader':
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close() 
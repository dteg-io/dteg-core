"""
Transformer 기본 인터페이스 정의
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import pandas as pd


class BaseTransformer(ABC):
    """데이터 변환을 위한 기본 추상 클래스"""

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Transformer 초기화

        Args:
            config: Transformer 설정
        """
        self.config = config
        self.initialize()

    def initialize(self) -> None:
        """
        추가 초기화 작업 수행
        하위 클래스에서 필요한 경우 오버라이드
        """
        pass

    @abstractmethod
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        데이터 변환 실행

        Args:
            data: 변환할 원본 데이터

        Returns:
            변환된 데이터
        """
        pass

    def validate_schema(self, data: pd.DataFrame) -> None:
        """
        데이터 스키마 검증

        Args:
            data: 검증할 데이터

        Raises:
            ValueError: 스키마가 유효하지 않은 경우
        """
        # 기본 구현은 항상 유효함을 가정
        pass

    def get_metadata(self) -> Dict[str, Any]:
        """
        Transformer 메타데이터 반환

        Returns:
            메타데이터 딕셔너리
        """
        return {
            "type": self.__class__.__name__,
            "config": {k: v for k, v in self.config.items() if k != "password"}
        }

    def cleanup(self) -> None:
        """
        자원 정리 작업 수행
        하위 클래스에서 필요한 경우 오버라이드
        """
        pass 
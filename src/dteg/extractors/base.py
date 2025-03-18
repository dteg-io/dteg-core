"""
데이터 추출을 위한 기본 인터페이스
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, Generator, List, Optional

import pandas as pd


class Extractor(ABC):
    """데이터 소스에서 데이터를 추출하기 위한 추상 기본 클래스

    모든 Extractor 구현체는 이 클래스를 상속해야 합니다.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Args:
            config: Extractor 설정
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
        """소스 연결 설정"""
        pass

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """데이터 추출 실행

        Returns:
            추출된 데이터를 포함하는 DataFrame

        Raises:
            RuntimeError: 데이터 추출 중 오류 발생
        """
        pass

    def extract_batch(
        self, batch_size: int = 10000
    ) -> Generator[pd.DataFrame, None, None]:
        """데이터를 배치로 추출

        Args:
            batch_size: 각 배치의 최대 행 수

        Yields:
            추출된 데이터의 각 배치
        """
        # 기본 구현은 전체 데이터를 추출한 다음 배치로 분할
        # 하위 클래스에서 더 효율적인 배치 추출 방법 구현 가능
        data = self.extract()
        for i in range(0, len(data), batch_size):
            yield data.iloc[i : i + batch_size]

    def get_schema(self) -> List[Dict[str, Any]]:
        """추출할 데이터의 스키마 반환

        Returns:
            스키마 정보 (열 이름, 타입 등)
        """
        # 샘플 데이터로 스키마 추론
        sample_data = self.extract_sample()
        schema = []
        for col_name, dtype in sample_data.dtypes.items():
            schema.append({"name": col_name, "type": str(dtype)})
        return schema

    def extract_sample(self, n: int = 5) -> pd.DataFrame:
        """샘플 데이터 추출

        Args:
            n: 샘플 행 수

        Returns:
            샘플 데이터를 포함하는 DataFrame
        """
        # 기본 구현은 일반 추출 메서드를 호출하고 처음 n개 행 반환
        # 하위 클래스에서 더 효율적인 샘플링 방법 구현 가능
        try:
            data = next(self.extract_batch(n))
            return data.head(n)
        except StopIteration:
            # 데이터가 없는 경우 빈 DataFrame 반환
            return pd.DataFrame()

    def close(self) -> None:
        """소스 연결 종료"""
        # 기본적으로 아무 작업도 수행하지 않음
        # 필요한 경우 하위 클래스에서 재정의
        pass

    def __enter__(self) -> 'Extractor':
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close() 
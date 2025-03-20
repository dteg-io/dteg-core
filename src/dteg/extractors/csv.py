"""
CSV 파일에서 데이터를 추출하기 위한 Extractor 구현
"""
import glob
import os
from typing import Any, Dict, Generator, List, Optional, Union

import pandas as pd

from dteg.extractors.base import Extractor


class CSVExtractor(Extractor):
    """CSV 파일에서 데이터를 추출하는 Extractor

    설정 매개변수:
        file_path: 파일 경로 (glob 패턴 지원, 예: /data/*.csv)
        delimiter: 필드 구분자 (기본값: ",")
        encoding: 파일 인코딩 (기본값: "utf-8")
        header: 헤더 포함 여부 (기본값: True)
        dtype: 데이터 타입 명시 (예: {"column1": "int64", "column2": "float"})
        parse_dates: 날짜 형식으로 파싱할 컬럼 리스트 (예: ["date", "timestamp"])
        skip_rows: 건너뛸 행 수
        nrows: 읽을 최대 행 수
        usecols: 읽을 컬럼 리스트
    """

    # 플러그인 등록용 타입 식별자
    TYPE = "csv"

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Args:
            config: Extractor 설정
        """
        super().__init__(config)
        self.file_paths = []
        self.current_file_index = 0

    def _validate_config(self) -> None:
        """설정 유효성 검사

        Raises:
            ValueError: 필수 설정이 누락되었거나 잘못된 경우
        """
        if "file_path" not in self.config:
            raise ValueError("CSV Extractor 필수 설정 누락: file_path")

    def _setup(self) -> None:
        """파일 목록 설정"""
        # glob 패턴으로 파일 목록 가져오기
        file_path = self.config["file_path"]
        self.file_paths = sorted(glob.glob(os.path.expanduser(file_path)))
        
        # 테스트용이 아닌 실제 환경에서는 파일이 없으면 오류를 발생시킵니다
        if not self.file_paths and not os.path.exists(file_path):
            raise ValueError(f"파일 패턴과 일치하는 파일이 없습니다: {file_path}")

    def _read_csv_file(self, file_path: str) -> pd.DataFrame:
        """CSV 파일 읽기

        Args:
            file_path: 읽을 CSV 파일 경로

        Returns:
            CSV 데이터를 포함하는 DataFrame
        """
        return pd.read_csv(
            file_path,
            delimiter=self.config.get("delimiter", ","),
            encoding=self.config.get("encoding", "utf-8"),
            header=0 if self.config.get("header", True) else None,
            dtype=self.config.get("dtype"),
            parse_dates=self.config.get("parse_dates"),
            skiprows=self.config.get("skip_rows"),
            nrows=self.config.get("nrows"),
            usecols=self.config.get("usecols"),
            low_memory=self.config.get("low_memory", False)
        )

    def extract(self) -> pd.DataFrame:
        """CSV 파일에서 데이터 추출

        파일 경로가 여러 파일과 일치하는 경우 모든 파일의 데이터를 결합합니다.

        Returns:
            추출된 데이터를 포함하는 DataFrame

        Raises:
            RuntimeError: 데이터 추출 중 오류 발생
        """
        try:
            self._setup()  # 파일 목록 설정
            dataframes = []
            
            for file_path in self.file_paths:
                df = self._read_csv_file(file_path)
                dataframes.append(df)
                
            if not dataframes:
                return pd.DataFrame()
                
            # 모든 DataFrame 결합
            return pd.concat(dataframes, ignore_index=True)
            
        except Exception as e:
            raise RuntimeError(f"CSV 데이터 추출 중 오류 발생: {e}")

    def extract_batch(
        self, batch_size: Optional[int] = None
    ) -> Generator[pd.DataFrame, None, None]:
        """CSV 파일 데이터를 배치 단위로 추출

        여러 파일이 있는 경우, 각 파일을 하나의 배치로 처리합니다.
        파일이 매우 큰 경우, 각 파일 내에서도 배치 처리를 수행합니다.

        Args:
            batch_size: 각 배치의 크기 (기본값: config에서 지정한 값 또는 10000)

        Yields:
            추출된 데이터의 각 배치
        """
        if batch_size is None:
            batch_size = self.config.get("batch_size", 10000)

        try:
            self._setup()  # 파일 목록 설정
            # 대용량 파일 처리를 위한 청크 단위 읽기
            for file_path in self.file_paths:
                for chunk in pd.read_csv(
                    file_path,
                    delimiter=self.config.get("delimiter", ","),
                    encoding=self.config.get("encoding", "utf-8"),
                    header=0 if self.config.get("header", True) else None,
                    dtype=self.config.get("dtype"),
                    parse_dates=self.config.get("parse_dates"),
                    skiprows=self.config.get("skip_rows"),
                    chunksize=batch_size,
                    usecols=self.config.get("usecols"),
                    low_memory=self.config.get("low_memory", False)
                ):
                    yield chunk
        except Exception as e:
            raise RuntimeError(f"CSV 배치 데이터 추출 중 오류 발생: {e}")

    def extract_sample(self, n: int = 5) -> pd.DataFrame:
        """샘플 데이터 추출

        첫 번째 CSV 파일에서 n개 행을 샘플로 추출합니다.

        Args:
            n: 샘플 행 수

        Returns:
            샘플 데이터를 포함하는 DataFrame
        """
        try:
            self._setup()  # 파일 목록 설정
            
            if not self.file_paths:
                return pd.DataFrame()
            
            # 첫 번째 파일에서 n개 행만 읽기
            return pd.read_csv(
                self.file_paths[0],
                delimiter=self.config.get("delimiter", ","),
                encoding=self.config.get("encoding", "utf-8"),
                header=0 if self.config.get("header", True) else None,
                nrows=n,
                dtype=self.config.get("dtype"),
                parse_dates=self.config.get("parse_dates"),
                skiprows=self.config.get("skip_rows"),
                usecols=self.config.get("usecols"),
                low_memory=self.config.get("low_memory", False)
            )
        except Exception as e:
            raise RuntimeError(f"CSV 샘플 데이터 추출 중 오류 발생: {e}") 
"""
데이터를 CSV 파일로 저장하기 위한 Loader 구현
"""
import os
import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from dteg.loaders.base import Loader, IfExists


class CSVLoader(Loader):
    """데이터를 CSV 파일로 저장하는 Loader

    설정 매개변수:
        file_path: 저장할 파일 경로
        delimiter: 필드 구분자 (기본값: ",")
        encoding: 파일 인코딩 (기본값: "utf-8")
        header: 헤더 포함 여부 (기본값: True)
        index: 인덱스 포함 여부 (기본값: False)
        if_exists: 파일이 이미 존재할 때 처리 방법 (기본값: "replace")
            - fail: 파일이 이미 존재하면 오류 발생
            - replace: 기존 파일을 덮어씀
            - append: 기존 파일에 데이터 추가
        date_format: 날짜/시간 포맷 (예: "%Y-%m-%d")
        float_format: 부동 소수점 포맷 (예: "%.2f")
        compression: 압축 방식 (예: "gzip", "zip")
    """

    # 플러그인 등록용 타입 식별자
    TYPE = "csv"

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Args:
            config: Loader 설정
        """
        super().__init__(config)
        self.logger = logging.getLogger(__name__)

    def _validate_config(self) -> None:
        """설정 유효성 검사

        Raises:
            ValueError: 필수 설정이 누락되었거나 잘못된 경우
        """
        if "file_path" not in self.config:
            raise ValueError("CSV Loader 필수 설정 누락: file_path")
        
        # if_exists 값 검증
        if "if_exists" in self.config:
            if self.config["if_exists"] not in [item.value for item in IfExists]:
                raise ValueError(f"잘못된 if_exists 값: {self.config['if_exists']}. 유효한 값: {[item.value for item in IfExists]}")

    def _setup(self) -> None:
        """파일 경로 설정"""
        # 파일 디렉토리가 존재하지 않으면 생성
        file_path = self.config["file_path"]
        directory = os.path.dirname(os.path.abspath(os.path.expanduser(file_path)))
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

    def load(self, data: pd.DataFrame) -> int:
        """데이터를 CSV 파일로 저장

        Args:
            data: 저장할 데이터

        Returns:
            저장된 행 수

        Raises:
            ValueError: 파일이 이미 존재하고 if_exists='fail'인 경우
            RuntimeError: 데이터 저장 중 기타 오류 발생
        """
        file_path = self.config["file_path"]
        
        # 파일 존재 여부 확인 및 처리
        file_exists = os.path.exists(file_path)
        if_exists = self.config.get("if_exists", IfExists.REPLACE.value)
        
        # ValueError는 그대로 전파하기 위해 try 블록 밖으로 이동
        if file_exists and if_exists == IfExists.FAIL.value:
            raise ValueError(f"파일이 이미 존재합니다: {file_path}")
        
        try:
            # 파일 저장 모드 결정
            mode = 'a' if file_exists and if_exists == IfExists.APPEND.value else 'w'
            
            # 헤더 결정 (추가 모드에서는 헤더 생략)
            header = self.config.get("header", True)
            if file_exists and if_exists == IfExists.APPEND.value:
                header = False
            
            # CSV 파일로 저장
            data.to_csv(
                file_path,
                sep=self.config.get("delimiter", ","),
                encoding=self.config.get("encoding", "utf-8"),
                index=self.config.get("index", False),
                header=header,
                mode=mode,
                date_format=self.config.get("date_format"),
                float_format=self.config.get("float_format"),
                compression=self.config.get("compression"),
                quoting=self.config.get("quoting"),
                quotechar=self.config.get("quotechar", '"'),
                lineterminator=self.config.get("lineterminator", '\n'),
                escapechar=self.config.get("escapechar"),
                doublequote=self.config.get("doublequote", True)
            )
            
            return len(data)
        except Exception as e:
            raise RuntimeError(f"CSV 파일 저장 중 오류 발생: {e}")

    def create_if_not_exists(self, data: pd.DataFrame) -> bool:
        """파일이 존재하지 않는 경우 빈 파일 생성

        Args:
            data: 적재할 데이터의 샘플(스키마 추론용)

        Returns:
            새로 생성되었으면 True, 이미 존재했으면 False
        """
        file_path = self.config["file_path"]
        if os.path.exists(file_path):
            return False
        
        try:
            # 빈 데이터프레임 저장 (헤더만 있는 파일)
            empty_df = pd.DataFrame(columns=data.columns)
            empty_df.to_csv(
                file_path,
                sep=self.config.get("delimiter", ","),
                encoding=self.config.get("encoding", "utf-8"),
                index=self.config.get("index", False)
            )
            return True
        except Exception as e:
            raise RuntimeError(f"빈 CSV 파일 생성 중 오류 발생: {e}") 
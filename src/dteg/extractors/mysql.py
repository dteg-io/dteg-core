"""
MySQL 데이터베이스에서 데이터를 추출하기 위한 Extractor 구현
"""
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

import pandas as pd
import pymysql
from pymysql.cursors import DictCursor

from dteg.extractors.base import Extractor


class MySQLExtractor(Extractor):
    """MySQL 데이터베이스에서 데이터를 추출하는 Extractor

    설정 매개변수:
        host: MySQL 서버 호스트
        port: MySQL 서버 포트 (기본값: 3306)
        database: 데이터베이스 이름
        user: 데이터베이스 사용자
        password: 데이터베이스 비밀번호
        query: 실행할 SQL 쿼리 (table과 함께 사용 불가)
        table: 데이터를 추출할 테이블 (query와 함께 사용 불가)
        columns: 추출할 컬럼 목록 (기본값: "*")
        where: WHERE 절 조건 (table과 함께 사용)
        limit: 최대 추출 행 수
        batch_size: 배치당 추출할 행 수 (기본값: 10000)
        charset: 문자셋 (기본값: "utf8mb4")
        retry_count: 연결 재시도 횟수 (기본값: 3)
        connect_timeout: 연결 타임아웃 (초, 기본값: 10)
    """

    # 플러그인 등록용 타입 식별자
    TYPE = "mysql"

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Args:
            config: Extractor 설정
        """
        super().__init__(config)
        self.connection = None
        self.query = None

    def _validate_config(self) -> None:
        """설정 유효성 검사

        Raises:
            ValueError: 필수 설정이 누락되었거나 잘못된 경우
        """
        required_fields = ["host", "database", "user", "password"]
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"MySQL Extractor 필수 설정 누락: {field}")

        # query 또는 table 중 하나는 필수
        if "query" not in self.config and "table" not in self.config:
            raise ValueError("MySQL Extractor는 'query' 또는 'table' 설정이 필요합니다")

        # query와 table은 함께 사용 불가
        if "query" in self.config and "table" in self.config:
            raise ValueError("MySQL Extractor는 'query'와 'table'을 함께 사용할 수 없습니다")

    def _setup(self) -> None:
        """MySQL 연결 설정 및 쿼리 준비"""
        # 쿼리 준비
        if "query" in self.config:
            self.query = self.config["query"]
        else:
            # table로부터 쿼리 구성
            columns = self.config.get("columns", "*")
            if isinstance(columns, list):
                columns = ", ".join(columns)
            
            table = self.config["table"]
            
            self.query = f"SELECT {columns} FROM {table}"
            
            if "where" in self.config:
                self.query += f" WHERE {self.config['where']}"
            
            if "limit" in self.config:
                self.query += f" LIMIT {self.config['limit']}"

    def _get_connection(self) -> pymysql.Connection:
        """MySQL 데이터베이스 연결 생성 또는 재사용

        Returns:
            MySQL 연결 객체

        Raises:
            RuntimeError: 연결 실패 시
        """
        retry_count = 0
        max_retries = self.config.get("retry_count", 3)
        last_error = None

        while retry_count <= max_retries:
            try:
                if self.connection is None or not self.connection.open:
                    self.connection = pymysql.connect(
                        host=self.config["host"],
                        port=self.config.get("port", 3306),
                        user=self.config["user"],
                        password=self.config["password"],
                        database=self.config["database"],
                        charset=self.config.get("charset", "utf8mb4"),
                        connect_timeout=self.config.get("connect_timeout", 10),
                        cursorclass=DictCursor  # 딕셔너리 형태로 결과 반환
                    )
                return self.connection
            except (pymysql.MySQLError, pymysql.Error) as e:
                retry_count += 1
                last_error = e
                
                if retry_count > max_retries:
                    break
                    
                # 지수 백오프 (exponential backoff) 적용하여 재시도 간격 증가
                import time
                time.sleep(2 ** retry_count)
        
        # 모든 재시도 실패
        raise RuntimeError(f"MySQL 연결 실패 (최대 재시도 횟수 초과): {last_error}")

    def extract(self) -> pd.DataFrame:
        """MySQL 데이터베이스에서 데이터 추출

        Returns:
            추출된 데이터를 포함하는 DataFrame

        Raises:
            RuntimeError: 데이터 추출 중 오류 발생
        """
        try:
            self._setup()  # MySQL 연결 및 쿼리 준비
            connection = self._get_connection()
            with connection.cursor() as cursor:
                cursor.execute(self.query)
                data = cursor.fetchall()
                return pd.DataFrame(data)
        except Exception as e:
            raise RuntimeError(f"MySQL 데이터 추출 중 오류 발생: {e}")

    def extract_batch(
        self, batch_size: Optional[int] = None
    ) -> Generator[pd.DataFrame, None, None]:
        """MySQL 데이터베이스에서 배치 단위로 데이터 추출

        Args:
            batch_size: 각 배치의 크기 (기본값: config에서 지정한 값 또는 10000)

        Yields:
            추출된 데이터의 각 배치
        """
        if batch_size is None:
            batch_size = self.config.get("batch_size", 10000)

        try:
            self._setup()  # MySQL 연결 및 쿼리 준비
            connection = self._get_connection()
            with connection.cursor() as cursor:
                # SSCursor 활용하여 서버 측 커서 사용 (메모리 효율성)
                cursor.execute(self.query)
                
                while True:
                    data = cursor.fetchmany(batch_size)
                    if not data:
                        break
                    yield pd.DataFrame(data)
        except Exception as e:
            raise RuntimeError(f"MySQL 배치 데이터 추출 중 오류 발생: {e}")

    def extract_sample(self, n: int = 5) -> pd.DataFrame:
        """샘플 데이터 추출

        원본 쿼리에 LIMIT 절을 추가하여 샘플 데이터만 가져옵니다.

        Args:
            n: 샘플 행 수

        Returns:
            샘플 데이터를 포함하는 DataFrame
        """
        try:
            self._setup()  # MySQL 연결 및 쿼리 준비
            connection = self._get_connection()
            with connection.cursor() as cursor:
                # 원본 쿼리에 LIMIT 추가 (이미 LIMIT이 있는 경우 처리)
                sample_query = self.query
                if "LIMIT" in sample_query.upper():
                    # 기존 LIMIT 절을 찾아서 수정
                    import re
                    sample_query = re.sub(r"LIMIT\s+\d+", f"LIMIT {n}", sample_query, flags=re.IGNORECASE)
                else:
                    sample_query += f" LIMIT {n}"
                
                cursor.execute(sample_query)
                data = cursor.fetchall()
                return pd.DataFrame(data)
        except Exception as e:
            raise RuntimeError(f"MySQL 샘플 데이터 추출 중 오류 발생: {e}")

    def get_schema(self) -> List[Dict[str, Any]]:
        """데이터 스키마 정보 가져오기

        Returns:
            스키마 정보 (컬럼명, 타입 등)
        """
        try:
            connection = self._get_connection()
            
            # 테이블이 지정된 경우, 테이블 스키마 조회
            if "table" in self.config:
                with connection.cursor() as cursor:
                    cursor.execute(f"DESCRIBE {self.config['table']}")
                    schema_data = cursor.fetchall()
                    
                    schema = []
                    for column in schema_data:
                        schema.append({
                            "name": column["Field"],
                            "type": column["Type"],
                            "nullable": column["Null"] == "YES",
                            "key": column["Key"],
                            "default": column["Default"],
                            "extra": column["Extra"]
                        })
                    return schema
            
            # 쿼리인 경우, 샘플 데이터를 가져와서 스키마 추론
            return super().get_schema()
        except Exception as e:
            raise RuntimeError(f"MySQL 스키마 조회 중 오류 발생: {e}")

    def close(self) -> None:
        """MySQL 연결 종료"""
        if self.connection and self.connection.open:
            self.connection.close()
            self.connection = None 
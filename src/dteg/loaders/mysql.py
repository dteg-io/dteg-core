"""
데이터를 MySQL 데이터베이스에 저장하기 위한 Loader 구현
"""
import time
from typing import Any, Dict, List

import pandas as pd
import pymysql
from pymysql.cursors import DictCursor
from sqlalchemy import create_engine, inspect

from dteg.loaders.base import Loader, IfExists


class MySQLLoader(Loader):
    """데이터를 MySQL 데이터베이스에 저장하는 Loader

    설정 매개변수:
        host: MySQL 서버 호스트
        port: MySQL 서버 포트 (기본값: 3306)
        database: 데이터베이스 이름
        user: 데이터베이스 사용자
        password: 데이터베이스 비밀번호
        table: 데이터를 저장할 테이블
        if_exists: 테이블이 이미 존재할 때 처리 방법 (기본값: "append")
            - fail: 테이블이 이미 존재하면 오류 발생
            - replace: 기존 테이블을 삭제하고 새로 생성
            - append: 기존 테이블에 데이터 추가
            - truncate: 기존 테이블 데이터를 모두 삭제하고 새 데이터 추가
        schema: 스키마 이름 (MySQL에서는 데이터베이스와 동일)
        dtype: 컬럼별 데이터 타입 (예: {"column": "VARCHAR(255)"})
        batch_size: 한 번에 저장할 최대 행 수 (기본값: 10000)
        create_table_indexes: 테이블 생성 시 설정할 인덱스 목록
        create_table_primary_key: 테이블 생성 시 설정할 기본 키
        charset: 문자셋 (기본값: "utf8mb4")
        retry_count: 연결 재시도 횟수 (기본값: 3)
        connect_timeout: 연결 타임아웃 (초, 기본값: 10)
    """

    # 플러그인 등록용 타입 식별자
    TYPE = "mysql"

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Args:
            config: Loader 설정
        """
        # 상위 클래스에서 _setup을 호출하므로 초기화는 _setup 메서드 내에서 처리
        super().__init__(config)

    def _validate_config(self) -> None:
        """설정 유효성 검사

        Raises:
            ValueError: 필수 설정이 누락되었거나 잘못된 경우
        """
        required_fields = ["host", "database", "user", "password", "table"]
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"MySQL Loader 필수 설정 누락: {field}")

        # if_exists 값 검증
        if "if_exists" in self.config:
            if_exists = self.config["if_exists"]
            valid_options = [item.value for item in IfExists]
            if if_exists not in valid_options:
                raise ValueError(
                    f"잘못된 if_exists 값: {if_exists}. 유효한 값: {valid_options}"
                )

    def _setup(self) -> None:
        """MySQL 연결 및 엔진 설정"""
        # 초기화
        self.connection = None
        self.engine = None

        # 나중에 사용할 수 있도록 연결 정보 저장
        self.host = self.config["host"]
        self.port = self.config.get("port", 3306)
        self.database = self.config["database"]
        self.user = self.config["user"]
        self.password = self.config["password"]
        self.table = self.config["table"]
        self.charset = self.config.get("charset", "utf8mb4")

        # SQLAlchemy 엔진 생성
        connection_string = (
            f"mysql+pymysql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
            f"?charset={self.charset}"
        )
        self.engine = create_engine(connection_string)

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
                time.sleep(2 ** retry_count)

        # 모든 재시도 실패
        raise RuntimeError(f"MySQL 연결 실패 (최대 재시도 횟수 초과): {last_error}")

    def load(self, data: pd.DataFrame) -> int:
        """데이터를 MySQL 테이블에 저장

        Args:
            data: 저장할 데이터

        Returns:
            저장된 행 수

        Raises:
            RuntimeError: 데이터 저장 중 오류 발생
        """
        try:
            table = self.config["table"]
            if_exists = self.config.get("if_exists", IfExists.REPLACE.value)
            batch_size = self.config.get("batch_size", 10000)

            # truncate 모드 처리
            if if_exists == IfExists.TRUNCATE.value:
                connection = self._get_connection()
                with connection.cursor() as cursor:
                    cursor.execute(f"TRUNCATE TABLE {table}")
                connection.commit()
                if_exists = IfExists.APPEND.value  # truncate 후 append로 처리

            # SQLAlchemy를 통한 데이터 적재
            return self._load_with_sqlalchemy(
                data, table, if_exists, batch_size
            )
        except Exception as e:
            raise RuntimeError(f"MySQL 데이터 저장 중 오류 발생: {e}")

    def _load_with_sqlalchemy(
        self, data: pd.DataFrame, table: str, if_exists: str, batch_size: int
    ) -> int:
        """SQLAlchemy를 사용하여 데이터 적재

        Args:
            data: 저장할 데이터
            table: 테이블 이름
            if_exists: 테이블 존재 시 처리 방법
            batch_size: 배치 크기

        Returns:
            저장된 행 수
        """
        # dtype 설정 (있는 경우)
        dtype = self.config.get("dtype")

        # 데이터 저장
        data.to_sql(
            name=table,
            con=self.engine,
            if_exists=if_exists,
            index=False,
            schema=self.config.get("schema"),
            dtype=dtype,
            chunksize=batch_size,
            method='multi'  # 다중 INSERT 문 사용
        )

        return len(data)

    def create_if_not_exists(self, data: pd.DataFrame) -> bool:
        """테이블이 존재하지 않는 경우 테이블 생성

        Args:
            data: 적재할 데이터의 샘플(스키마 추론용)

        Returns:
            새로 생성되었으면 True, 이미 존재했으면 False

        Raises:
            RuntimeError: 테이블 생성 중 오류 발생
        """
        try:
            # SQLAlchemy의 inspect를 사용하여 테이블 존재 여부 확인
            inspector = inspect(self.engine)
            schema = self.config.get("schema")
            if inspector.has_table(self.table, schema=schema):
                return False  # 이미 존재함

            # 테이블 생성
            self._create_table_from_dataframe(data)
            return True
        except Exception as e:
            raise RuntimeError(f"MySQL 테이블 생성 중 오류 발생: {e}")

    def _create_table_from_dataframe(self, df: pd.DataFrame) -> None:
        """데이터프레임에서 테이블 생성

        Args:
            df: 스키마 추론용 데이터프레임
        """
        # 사용자 지정 데이터 타입이 있는 경우 사용, 없으면 자동 추론
        dtype = self.config.get("dtype", {})

        # 기본 키와 인덱스 설정
        primary_key = self.config.get("create_table_primary_key")
        indexes = self.config.get("create_table_indexes", [])

        # 빈 데이터프레임으로 테이블 생성
        empty_df = pd.DataFrame(columns=df.columns)

        # 테이블 생성
        empty_df.to_sql(
            name=self.table,
            con=self.engine,
            if_exists="fail",
            index=False,
            dtype=dtype
        )

        # 기본 키와 인덱스 추가
        connection = self._get_connection()
        with connection.cursor() as cursor:
            # 기본 키 추가
            if primary_key:
                if isinstance(primary_key, list):
                    primary_key = ", ".join(primary_key)
                cursor.execute(
                    f"ALTER TABLE {self.table} ADD PRIMARY KEY ({primary_key})"
                )

            # 인덱스 추가
            for index in indexes:
                if isinstance(index, dict):
                    # 인덱스 이름과 컬럼 지정 가능
                    idx_name = index.get("name", f"idx_{self.table}")
                    idx_columns = index.get("columns", [])
                    if isinstance(idx_columns, list):
                        idx_columns = ", ".join(idx_columns)
                    query = f"CREATE INDEX {idx_name} ON {self.table}"
                    query += f" ({idx_columns})"
                    cursor.execute(query)
                elif isinstance(index, list):
                    # 컬럼만 지정하는 경우
                    idx_columns = ", ".join(index)
                    query = f"CREATE INDEX idx_{self.table} ON {self.table}"
                    query += f" ({idx_columns})"
                    cursor.execute(query)
                elif isinstance(index, str):
                    # 단일 컬럼 인덱스
                    cursor.execute(
                        f"CREATE INDEX idx_{self.table} ON {self.table} ({index})"
                    )

        connection.commit()

    def get_current_schema(self) -> List[Dict[str, Any]]:
        """현재 테이블의 스키마 정보 조회

        Returns:
            스키마 정보 목록

        Raises:
            RuntimeError: 스키마 조회 중 오류 발생
        """
        try:
            # SQLAlchemy의 inspect를 사용하여 컬럼 정보 가져오기
            inspector = inspect(self.engine)
            schema = self.config.get("schema")
            if not inspector.has_table(self.table, schema=schema):
                msg = f"테이블이 존재하지 않습니다: {self.table}"
                raise ValueError(msg)

            columns = inspector.get_columns(self.table, schema=schema)

            # 스키마 정보 변환
            schema = []
            for column in columns:
                schema.append({
                    "name": column["name"],
                    "type": str(column["type"]),
                    "nullable": column.get("nullable", True),
                    "default": column.get("default"),
                })
            return schema
        except Exception as e:
            raise RuntimeError(f"MySQL 스키마 조회 중 오류 발생: {e}")

    def close(self) -> None:
        """MySQL 연결 종료"""
        if self.connection and self.connection.open:
            self.connection.close()
            self.connection = None
        
        if self.engine:
            self.engine.dispose()
            self.engine = None 
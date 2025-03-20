"""
SQL 기반 데이터 변환기 구현
"""
import re
import sqlite3
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from jinja2 import Template

from dteg.transformers.base import BaseTransformer
from dteg.utils.logging import get_logger

# 로거 초기화
logger = get_logger()


class SQLTransformer(BaseTransformer):
    """SQL 쿼리를 사용하여 데이터를 변환하는 Transformer"""

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        SQLTransformer 초기화

        Args:
            config: 설정 딕셔너리
                - query: SQL 쿼리 문자열 또는 쿼리 파일 경로
                - query_params: 쿼리에 전달할 파라미터 (선택)
                - temp_table: 임시 테이블 이름 (기본값: 'source_data')
                - engine: SQL 엔진 유형 ('sqlite', 'pandas', 'duckdb' 중 하나, 기본값: 'sqlite')
        """
        super().__init__(config)

    def initialize(self) -> None:
        """추가 초기화 작업 수행"""
        # 필수 설정 확인
        if "query" not in self.config:
            raise ValueError("SQL Transformer에 'query' 설정이 필요합니다")

        # 기본값 설정
        self.temp_table = self.config.get("temp_table", "source_data")
        self.engine = self.config.get("engine", "sqlite").lower()
        self.query_params = self.config.get("query_params", {})

        # 쿼리 로드
        self.query = self._load_query(self.config["query"])
        
        # 임시 데이터베이스 연결 설정 (필요한 경우)
        self.conn = None
        if self.engine == "sqlite":
            self.conn = sqlite3.connect(":memory:")

    def _load_query(self, query_or_path: str) -> str:
        """
        쿼리 문자열 또는 파일에서 쿼리 로드

        Args:
            query_or_path: SQL 쿼리 문자열 또는 쿼리 파일 경로

        Returns:
            로드된 SQL 쿼리
        """
        # 파일에서 쿼리 로드
        if query_or_path.endswith((".sql", ".jinja", ".j2")):
            try:
                with open(query_or_path, "r", encoding="utf-8") as f:
                    return f.read()
            except Exception as e:
                logger.error(f"SQL 쿼리 파일 로드 실패: {str(e)}")
                raise
        
        # 쿼리 문자열 사용
        return query_or_path

    def _render_template(self, query: str, params: Dict[str, Any]) -> str:
        """
        Jinja2 템플릿 렌더링

        Args:
            query: 템플릿 쿼리
            params: 템플릿 파라미터

        Returns:
            렌더링된 쿼리
        """
        try:
            template = Template(query)
            return template.render(**params)
        except Exception as e:
            logger.error(f"SQL 템플릿 렌더링 실패: {str(e)}")
            raise

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        SQL 쿼리를 사용하여 데이터 변환

        Args:
            data: 변환할 원본 데이터

        Returns:
            변환된 데이터
        """
        if data.empty:
            logger.warning("입력 데이터가 비어 있습니다. 변환하지 않고 반환합니다.")
            return data

        # 쿼리 렌더링
        rendered_query = self._render_template(self.query, self.query_params)
        logger.debug(f"렌더링된 SQL 쿼리: {rendered_query}")

        # 엔진별 처리
        if self.engine == "sqlite":
            return self._transform_sqlite(data, rendered_query)
        elif self.engine == "pandas":
            return self._transform_pandas(data, rendered_query)
        elif self.engine == "duckdb":
            return self._transform_duckdb(data, rendered_query)
        else:
            raise ValueError(f"지원하지 않는 SQL 엔진: {self.engine}")

    def _transform_sqlite(self, data: pd.DataFrame, query: str) -> pd.DataFrame:
        """
        SQLite를 사용한 변환

        Args:
            data: 원본 데이터
            query: SQL 쿼리

        Returns:
            변환된 데이터
        """
        try:
            # 데이터를 임시 테이블로 로드
            data.to_sql(self.temp_table, self.conn, if_exists="replace", index=False)
            
            # 쿼리 실행
            result = pd.read_sql_query(query, self.conn)
            
            return result
        except Exception as e:
            logger.error(f"SQLite 변환 실패: {str(e)}")
            raise

    def _transform_pandas(self, data: pd.DataFrame, query: str) -> pd.DataFrame:
        """
        Pandas query를 사용한 변환 (단순 필터링 쿼리에만 적합)

        Args:
            data: 원본 데이터
            query: SQL 쿼리

        Returns:
            변환된 데이터
        """
        try:
            # SELECT * FROM table WHERE condition 형태의 쿼리에서 condition 부분만 추출
            where_clause = re.search(r"WHERE\s+(.*)", query, re.IGNORECASE)
            if where_clause:
                condition = where_clause.group(1)
                # SQL -> Pandas 쿼리 형식으로 간단한 변환
                condition = condition.replace("=", "==").replace("<>", "!=")
                return data.query(condition)
            else:
                # WHERE 절이 없으면 전체 데이터 반환
                return data
        except Exception as e:
            logger.error(f"Pandas 쿼리 변환 실패: {str(e)}")
            # Pandas 쿼리가 실패하면 SQLite로 폴백
            logger.info("SQLite 엔진으로 대체하여 재시도합니다")
            return self._transform_sqlite(data, query)

    def _transform_duckdb(self, data: pd.DataFrame, query: str) -> pd.DataFrame:
        """
        DuckDB를 사용한 변환

        Args:
            data: 원본 데이터
            query: SQL 쿼리

        Returns:
            변환된 데이터
        """
        try:
            import duckdb
        except ImportError:
            logger.warning("DuckDB가 설치되어 있지 않습니다. SQLite로 대체합니다.")
            return self._transform_sqlite(data, query)
            
        try:
            # DuckDB에 데이터 등록
            duckdb_conn = duckdb.connect(database=":memory:")
            duckdb_conn.register(self.temp_table, data)
            
            # 쿼리 실행
            result = duckdb_conn.execute(query).fetchdf()
            duckdb_conn.close()
            
            return result
        except Exception as e:
            logger.error(f"DuckDB 변환 실패: {str(e)}")
            raise

    def cleanup(self) -> None:
        """리소스 정리"""
        if self.conn is not None:
            self.conn.close()
            self.conn = None
            
    def close(self) -> None:
        """리소스 정리 (cleanup의 별칭)"""
        self.cleanup() 
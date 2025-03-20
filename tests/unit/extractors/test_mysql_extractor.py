"""
MySQL Extractor 단위 테스트
"""
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
import pymysql
import pytest

from dteg.extractors.mysql import MySQLExtractor


class TestMySQLExtractor(unittest.TestCase):
    """MySQL Extractor 테스트"""

    def test_validate_config_required_fields(self):
        """필수 필드 검증 테스트"""
        # 필수 필드 누락
        with self.assertRaises(ValueError):
            MySQLExtractor({
                "host": "localhost",
                "database": "test_db",
                "user": "test_user",
                # password 누락
                "query": "SELECT * FROM test"
            })

    def test_validate_config_query_or_table(self):
        """query 또는 table 필수 검증 테스트"""
        # query와 table 모두 누락
        with self.assertRaises(ValueError):
            MySQLExtractor({
                "host": "localhost",
                "database": "test_db",
                "user": "test_user",
                "password": "password"
                # query 또는 table 누락
            })

        # query와 table 모두 지정 (충돌)
        with self.assertRaises(ValueError):
            MySQLExtractor({
                "host": "localhost",
                "database": "test_db",
                "user": "test_user",
                "password": "password",
                "query": "SELECT * FROM test",
                "table": "test"  # 충돌
            })

    def test_setup_with_query(self):
        """쿼리 설정 테스트"""
        extractor = MySQLExtractor({
            "host": "localhost",
            "database": "test_db",
            "user": "test_user",
            "password": "password",
            "query": "SELECT * FROM test WHERE id > 10"
        })
        
        extractor._setup()
        
        self.assertEqual(extractor.query, "SELECT * FROM test WHERE id > 10")

    def test_setup_with_table(self):
        """테이블 설정 테스트"""
        extractor = MySQLExtractor({
            "host": "localhost",
            "database": "test_db",
            "user": "test_user",
            "password": "password",
            "table": "customers",
            "columns": ["id", "name", "email"],
            "where": "status = 'active'",
            "limit": 100
        })
        
        extractor._setup()
        
        self.assertEqual(extractor.query, "SELECT id, name, email FROM customers WHERE status = 'active' LIMIT 100")

    def test_setup_with_table_default_columns(self):
        """테이블 설정 (기본 컬럼) 테스트"""
        extractor = MySQLExtractor({
            "host": "localhost",
            "database": "test_db",
            "user": "test_user",
            "password": "password",
            "table": "customers"
        })
        
        extractor._setup()
        
        self.assertEqual(extractor.query, "SELECT * FROM customers")

    @patch("pymysql.connect")
    def test_get_connection(self, mock_connect):
        """연결 생성 테스트"""
        # 모의 객체 설정
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        # Extractor 생성
        extractor = MySQLExtractor({
            "host": "localhost",
            "database": "test_db",
            "user": "test_user",
            "password": "password",
            "query": "SELECT * FROM test",
            "port": 3307,
            "charset": "utf8",
            "connect_timeout": 5
        })
        
        # 연결 가져오기
        connection = extractor._get_connection()
        
        # 검증
        mock_connect.assert_called_once_with(
            host="localhost",
            port=3307,
            user="test_user",
            password="password",
            database="test_db",
            charset="utf8",
            connect_timeout=5,
            cursorclass=pymysql.cursors.DictCursor
        )
        self.assertEqual(connection, mock_connection)

    @patch("pymysql.connect")
    def test_connection_retry(self, mock_connect):
        """연결 재시도 테스트"""
        # 첫 번째 호출에서는 예외 발생, 두 번째 호출에서는 성공
        mock_connection = MagicMock()
        mock_connect.side_effect = [
            pymysql.OperationalError("Connection error"),
            mock_connection
        ]
        
        # time.sleep 모의화 (테스트 속도 향상)
        with patch("time.sleep") as mock_sleep:
            # Extractor 생성 및 연결 시도
            extractor = MySQLExtractor({
                "host": "localhost",
                "database": "test_db",
                "user": "test_user",
                "password": "password",
                "query": "SELECT * FROM test",
                "retry_count": 1
            })
            
            connection = extractor._get_connection()
            
            # 검증
            self.assertEqual(mock_connect.call_count, 2)
            mock_sleep.assert_called_once()
            self.assertEqual(connection, mock_connection)

    @patch("pymysql.connect")
    def test_extract(self, mock_connect):
        """데이터 추출 테스트"""
        # 모의 객체 설정
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # 테스트 데이터
        test_data = [
            {"id": 1, "name": "Test 1"},
            {"id": 2, "name": "Test 2"}
        ]
        mock_cursor.fetchall.return_value = test_data
        
        # Extractor 생성 및 실행
        extractor = MySQLExtractor({
            "host": "localhost",
            "database": "test_db",
            "user": "test_user",
            "password": "password",
            "query": "SELECT * FROM test"
        })
        
        result = extractor.extract()
        
        # 검증
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test")
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]["name"], "Test 1")

    @patch("pymysql.connect")
    def test_extract_batch(self, mock_connect):
        """배치 데이터 추출 테스트"""
        # 모의 객체 설정
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # 배치 데이터 시뮬레이션
        batch1 = [{"id": 1, "name": "Test 1"}, {"id": 2, "name": "Test 2"}]
        batch2 = [{"id": 3, "name": "Test 3"}]
        empty_batch = []
        
        mock_cursor.fetchmany.side_effect = [batch1, batch2, empty_batch]
        
        # Extractor 생성 및 실행
        extractor = MySQLExtractor({
            "host": "localhost",
            "database": "test_db",
            "user": "test_user",
            "password": "password",
            "query": "SELECT * FROM test",
            "batch_size": 2
        })
        
        batches = list(extractor.extract_batch())
        
        # 검증
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test")
        self.assertEqual(len(batches), 2)
        self.assertEqual(len(batches[0]), 2)
        self.assertEqual(len(batches[1]), 1)
        self.assertEqual(batches[0].iloc[0]["name"], "Test 1")
        self.assertEqual(batches[1].iloc[0]["name"], "Test 3")

    @patch("pymysql.connect")
    def test_extract_sample(self, mock_connect):
        """샘플 데이터 추출 테스트"""
        # 모의 객체 설정
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # 테스트 데이터
        test_data = [{"id": 1, "name": "Test 1"}, {"id": 2, "name": "Test 2"}]
        mock_cursor.fetchall.return_value = test_data
        
        # Extractor 생성 및 실행
        extractor = MySQLExtractor({
            "host": "localhost",
            "database": "test_db",
            "user": "test_user",
            "password": "password",
            "query": "SELECT * FROM test"
        })
        
        # 쿼리 설정 (extract_sample 전에 필요)
        extractor._setup()
        
        result = extractor.extract_sample(2)
        
        # 검증
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test LIMIT 2")
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)

    @patch("pymysql.connect")
    def test_get_schema_from_table(self, mock_connect):
        """테이블 스키마 조회 테스트"""
        # 모의 객체 설정
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # DESCRIBE 결과 데이터
        schema_data = [
            {"Field": "id", "Type": "int(11)", "Null": "NO", "Key": "PRI", "Default": None, "Extra": "auto_increment"},
            {"Field": "name", "Type": "varchar(100)", "Null": "YES", "Key": "", "Default": None, "Extra": ""}
        ]
        mock_cursor.fetchall.return_value = schema_data
        
        # Extractor 생성 및 실행
        extractor = MySQLExtractor({
            "host": "localhost",
            "database": "test_db",
            "user": "test_user",
            "password": "password",
            "table": "users"
        })
        
        schema = extractor.get_schema()
        
        # 검증
        mock_cursor.execute.assert_called_once_with("DESCRIBE users")
        self.assertEqual(len(schema), 2)
        self.assertEqual(schema[0]["name"], "id")
        self.assertEqual(schema[0]["type"], "int(11)")
        self.assertEqual(schema[0]["nullable"], False)
        self.assertEqual(schema[1]["name"], "name")
        self.assertEqual(schema[1]["type"], "varchar(100)")
        self.assertEqual(schema[1]["nullable"], True)

    def test_close(self):
        """연결 종료 테스트"""
        with patch("pymysql.connect") as mock_connect:
            mock_connection = MagicMock()
            mock_connect.return_value = mock_connection
            
            extractor = MySQLExtractor({
                "host": "localhost",
                "database": "test_db",
                "user": "test_user",
                "password": "password",
                "query": "SELECT * FROM test"
            })
            
            # _get_connection 호출하여 연결 생성
            extractor._get_connection()
            
            # 종료
            extractor.close()
            
            # 검증
            mock_connection.close.assert_called_once() 
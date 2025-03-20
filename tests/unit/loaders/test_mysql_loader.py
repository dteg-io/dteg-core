"""
MySQL Loader 단위 테스트
"""
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd

from dteg.loaders.mysql import MySQLLoader
from dteg.loaders import IfExists


@patch('dteg.loaders.mysql.pymysql.connect')
@patch('dteg.loaders.mysql.create_engine')
class TestMySQLLoader(unittest.TestCase):
    """MySQL Loader 테스트"""

    def setUp(self):
        """테스트 설정"""
        self.config = {
            "host": "localhost",
            "port": 3306,
            "database": "test_db",
            "user": "test_user",
            "password": "test_password",
            "table": "test_table"
        }
        
        # 테스트용 데이터프레임
        self.test_data = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Test 1", "Test 2", "Test 3"],
            "value": [100, 200, 300]
        })

    def test_validate_config_required_fields(self, mock_create_engine, mock_connect):
        """필수 필드 검증 테스트"""
        # host 필드 누락
        with self.assertRaises(ValueError):
            loader = MySQLLoader({
                "port": 3306,
                "database": "test_db",
                "user": "test_user",
                "password": "test_password",
                "table": "test_table"
            })
            loader._validate_config()
        
        # database 필드 누락
        with self.assertRaises(ValueError):
            loader = MySQLLoader({
                "host": "localhost",
                "port": 3306,
                "user": "test_user",
                "password": "test_password",
                "table": "test_table"
            })
            loader._validate_config()
        
        # table 필드 누락
        with self.assertRaises(ValueError):
            loader = MySQLLoader({
                "host": "localhost",
                "port": 3306,
                "database": "test_db",
                "user": "test_user",
                "password": "test_password"
            })
            loader._validate_config()

    def test_validate_config_invalid_if_exists(self, mock_create_engine, mock_connect):
        """if_exists 필드 검증 테스트"""
        with self.assertRaises(ValueError):
            loader = MySQLLoader({
                **self.config,
                "if_exists": "invalid_option"
            })
            loader._validate_config()

    def test_setup(self, mock_create_engine, mock_connect):
        """_setup 메서드 테스트"""
        # 호출 횟수 초기화
        mock_create_engine.reset_mock()
        
        # SQLAlchemy 엔진 모킹
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # 생성자에서 _setup이 호출됨
        loader = MySQLLoader(self.config)
        
        # create_engine이 올바른 인자로 호출되었는지 확인
        mock_create_engine.assert_called_once_with(
            f"mysql+pymysql://{self.config['user']}:{self.config['password']}"
            f"@{self.config['host']}:{self.config.get('port', 3306)}/{self.config['database']}"
            f"?charset={self.config.get('charset', 'utf8mb4')}"
        )
        
        # engine 속성이 설정되었는지 확인
        self.assertEqual(loader.engine, mock_engine)

    def test_load_basic(self, mock_create_engine, mock_connect):
        """기본 데이터 적재 테스트"""
        # SQLAlchemy 엔진과 연결 모킹
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # to_sql 메서드 모킹
        mock_to_sql = MagicMock()
        self.test_data.to_sql = mock_to_sql
        mock_to_sql.return_value = 3  # 3행이 삽입됨
        
        loader = MySQLLoader(self.config)
        result = loader.load(self.test_data)
        
        # to_sql이 올바른 인자로 호출되었는지 확인
        mock_to_sql.assert_called_once()
        call_args = mock_to_sql.call_args[1]
        self.assertEqual(call_args["name"], "test_table")
        self.assertEqual(call_args["con"], mock_engine)
        self.assertEqual(call_args["if_exists"], "replace")
        
        # 적재된 행 수 확인
        self.assertEqual(result, 3)

    def test_load_truncate(self, mock_create_engine, mock_connect):
        """테이블 truncate 테스트"""
        # SQLAlchemy 엔진과 연결 모킹
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # connection 모킹
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value.__enter__.return_value = mock_cursor
        
        # to_sql 메서드 모킹
        mock_to_sql = MagicMock()
        self.test_data.to_sql = mock_to_sql
        
        # truncate 모드 테스트
        loader = MySQLLoader({
            **self.config,
            "if_exists": IfExists.TRUNCATE.value
        })
        loader.load(self.test_data)
        
        # execute가 호출되었는지 확인 (TRUNCATE TABLE 명령)
        mock_cursor.execute.assert_called_once()
        truncate_call = mock_cursor.execute.call_args[0][0]
        self.assertIn("TRUNCATE TABLE", str(truncate_call))

    def test_load_with_options(self, mock_create_engine, mock_connect):
        """다양한 옵션으로 데이터 적재 테스트"""
        # SQLAlchemy 엔진과 연결 모킹
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # to_sql 메서드 모킹
        mock_to_sql = MagicMock()
        self.test_data.to_sql = mock_to_sql
        
        # 다양한 옵션으로 로더 생성
        loader = MySQLLoader({
            **self.config,
            "if_exists": IfExists.APPEND.value,
            "batch_size": 1000,
            "dtype": {"name": "VARCHAR(100)"}
        })
        loader.load(self.test_data)
        
        # to_sql이 올바른 인자로 호출되었는지 확인
        mock_to_sql.assert_called_once()
        call_args = mock_to_sql.call_args[1]
        self.assertEqual(call_args["name"], "test_table")
        self.assertEqual(call_args["con"], mock_engine)
        self.assertEqual(call_args["if_exists"], "append")
        self.assertEqual(call_args["chunksize"], 1000)
        self.assertEqual(call_args["dtype"], {"name": "VARCHAR(100)"})

    def test_create_if_not_exists(self, mock_create_engine, mock_connect):
        """테이블 없을 경우 생성 테스트"""
        # SQLAlchemy 엔진과 연결 모킹
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # inspector 모킹
        mock_inspector = MagicMock()
        mock_inspector.has_table.return_value = False
        
        # pymysql 커서 모킹
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value.__enter__.return_value = mock_cursor
        
        # to_sql 메서드 모킹 - DataFrame의 인스턴스 메서드를 직접 패치
        with patch('pandas.DataFrame.to_sql') as mock_to_sql:
            with patch('dteg.loaders.mysql.inspect', return_value=mock_inspector):
                loader = MySQLLoader(self.config)
                result = loader.create_if_not_exists(self.test_data)
                
                # 테이블이 없으므로 생성 시도
                self.assertTrue(result)
                mock_to_sql.assert_called_once()

    def test_create_if_not_exists_already_exists(self, mock_create_engine, mock_connect):
        """테이블이 이미 존재할 경우 테스트"""
        # SQLAlchemy 엔진과 연결 모킹
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # inspector 모킹
        mock_inspector = MagicMock()
        mock_inspector.has_table.return_value = True
        
        with patch('dteg.loaders.mysql.inspect', return_value=mock_inspector):
            loader = MySQLLoader(self.config)
            result = loader.create_if_not_exists(self.test_data)
            
            # 테이블이 이미 있으므로 생성 안 함
            self.assertFalse(result)

    def test_create_table_with_primary_key(self, mock_create_engine, mock_connect):
        """기본 키로 테이블 생성 테스트"""
        # SQLAlchemy 엔진과 연결 모킹
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # inspector 모킹
        mock_inspector = MagicMock()
        mock_inspector.has_table.return_value = False
        
        # pymysql 커서 모킹
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value.__enter__.return_value = mock_cursor
        
        # to_sql 메서드 모킹 - DataFrame의 인스턴스 메서드를 직접 패치
        with patch('pandas.DataFrame.to_sql') as mock_to_sql:
            with patch('dteg.loaders.mysql.inspect', return_value=mock_inspector):
                loader = MySQLLoader({
                    **self.config,
                    "create_table_primary_key": "id"
                })
                loader.create_if_not_exists(self.test_data)
                
                # to_sql이 호출되었는지 확인
                mock_to_sql.assert_called_once()
                # 기본 키가 추가되었는지 확인
                mock_cursor.execute.assert_called_with(
                    "ALTER TABLE test_table ADD PRIMARY KEY (id)"
                )

    def test_get_current_schema(self, mock_create_engine, mock_connect):
        """현재 스키마 조회 테스트"""
        # SQLAlchemy 엔진과 연결 모킹
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # inspector 모킹
        mock_inspector = MagicMock()
        mock_columns = [
            {"name": "id", "type": "INTEGER", "primary_key": True},
            {"name": "name", "type": "VARCHAR(255)"},
            {"name": "value", "type": "INTEGER"}
        ]
        mock_inspector.get_columns.return_value = mock_columns
        
        with patch('dteg.loaders.mysql.inspect', return_value=mock_inspector):
            loader = MySQLLoader(self.config)
            schema = loader.get_current_schema()
            
            # 스키마 정보 확인
            self.assertEqual(len(schema), 3)
            self.assertEqual(schema[0]["name"], "id")
            self.assertEqual(schema[1]["name"], "name")
            self.assertEqual(schema[2]["name"], "value")

    def test_close(self, mock_create_engine, mock_connect):
        """연결 종료 테스트"""
        # SQLAlchemy 엔진과 연결 모킹
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        loader = MySQLLoader(self.config)
        loader._setup()
        loader.close()
        
        # dispose가 호출되었는지 확인
        mock_engine.dispose.assert_called_once()


if __name__ == "__main__":
    unittest.main() 
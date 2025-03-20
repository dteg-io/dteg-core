"""
CSV Loader 단위 테스트
"""
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from dteg.loaders.base import IfExists
from dteg.loaders.csv import CSVLoader


class TestCSVLoader(unittest.TestCase):
    """CSV Loader 테스트"""

    def setUp(self):
        """테스트 환경 설정"""
        # 임시 디렉토리 생성
        self.temp_dir = tempfile.TemporaryDirectory()
        self.output_file = os.path.join(self.temp_dir.name, "output.csv")
        
        # 테스트용 데이터프레임 생성
        self.test_data = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Test 1", "Test 2", "Test 3"],
            "value": [100, 200, 300]
        })

    def tearDown(self):
        """테스트 종료 후 임시 파일 정리"""
        self.temp_dir.cleanup()

    def test_validate_config_required_fields(self):
        """필수 필드 검증 테스트"""
        # file_path 누락
        with self.assertRaises(ValueError):
            CSVLoader({})

    def test_validate_config_invalid_if_exists(self):
        """잘못된 if_exists 값 검증 테스트"""
        with self.assertRaises(ValueError):
            CSVLoader({
                "file_path": self.output_file,
                "if_exists": "invalid_option"
            })

    def test_load_basic(self):
        """기본 데이터 저장 테스트"""
        loader = CSVLoader({
            "file_path": self.output_file
        })
        
        # 데이터 저장
        row_count = loader.load(self.test_data)
        
        # 검증
        self.assertEqual(row_count, 3)  # 3개 행이 저장됨
        self.assertTrue(os.path.exists(self.output_file))
        
        # 저장된 파일 내용 확인
        saved_data = pd.read_csv(self.output_file)
        self.assertEqual(len(saved_data), 3)
        self.assertEqual(list(saved_data.columns), ["id", "name", "value"])
        self.assertEqual(saved_data.iloc[0]["name"], "Test 1")
        self.assertEqual(saved_data.iloc[2]["value"], 300)

    def test_load_with_options(self):
        """다양한 옵션 테스트"""
        loader = CSVLoader({
            "file_path": self.output_file,
            "delimiter": ";",
            "index": True,
            "header": False
        })
        
        # 데이터 저장
        loader.load(self.test_data)
        
        # 옵션이 적용되었는지 확인
        with open(self.output_file, "r") as f:
            content = f.read()
            self.assertIn(";", content)  # 구분자가 세미콜론인지 확인
            
        # 저장된 파일 내용 확인 (헤더 없음)
        saved_data = pd.read_csv(
            self.output_file, 
            delimiter=";", 
            header=None
        )
        self.assertEqual(len(saved_data), 3)

    def test_if_exists_fail(self):
        """fail 모드 테스트"""
        # 먼저 파일 생성
        pd.DataFrame({"test": [1]}).to_csv(self.output_file, index=False)
        
        # fail 모드로 로더 생성
        loader = CSVLoader({
            "file_path": self.output_file,
            "if_exists": IfExists.FAIL.value
        })
        
        # 이미 존재하므로 오류 발생해야 함
        with self.assertRaises(ValueError):
            loader.load(self.test_data)

    def test_if_exists_append(self):
        """append 모드 테스트"""
        # 먼저 첫 번째 데이터 저장
        initial_data = pd.DataFrame({
            "id": [1, 2],
            "name": ["Test 1", "Test 2"],
            "value": [100, 200]
        })
        initial_data.to_csv(self.output_file, index=False)
        
        # append 모드로 로더 생성
        loader = CSVLoader({
            "file_path": self.output_file,
            "if_exists": IfExists.APPEND.value
        })
        
        # 추가 데이터 저장
        additional_data = pd.DataFrame({
            "id": [3, 4],
            "name": ["Test 3", "Test 4"],
            "value": [300, 400]
        })
        loader.load(additional_data)
        
        # 저장된 파일 내용 확인
        saved_data = pd.read_csv(self.output_file)
        self.assertEqual(len(saved_data), 4)  # 총 4개 행이 있어야 함
        self.assertEqual(saved_data.iloc[3]["name"], "Test 4")

    def test_if_exists_replace(self):
        """replace 모드 테스트"""
        # 먼저 첫 번째 데이터 저장
        initial_data = pd.DataFrame({
            "id": [1, 2],
            "name": ["Initial 1", "Initial 2"],
            "value": [100, 200]
        })
        initial_data.to_csv(self.output_file, index=False)
        
        # replace 모드로 로더 생성
        loader = CSVLoader({
            "file_path": self.output_file,
            "if_exists": IfExists.REPLACE.value
        })
        
        # 새 데이터 저장
        loader.load(self.test_data)
        
        # 저장된 파일 내용 확인
        saved_data = pd.read_csv(self.output_file)
        self.assertEqual(len(saved_data), 3)  # 새 데이터만 있어야 함
        self.assertEqual(saved_data.iloc[0]["name"], "Test 1")

    def test_create_if_not_exists(self):
        """빈 파일 생성 테스트"""
        loader = CSVLoader({
            "file_path": self.output_file
        })
        
        # 파일이 존재하지 않으므로 새로 생성
        result = loader.create_if_not_exists(self.test_data)
        self.assertTrue(result)
        self.assertTrue(os.path.exists(self.output_file))
        
        # 이미 생성했으므로 False 반환
        result = loader.create_if_not_exists(self.test_data)
        self.assertFalse(result)

    def test_create_nested_directory(self):
        """중첩 디렉토리 생성 테스트"""
        nested_path = os.path.join(self.temp_dir.name, "nested", "dir", "output.csv")
        
        loader = CSVLoader({
            "file_path": nested_path
        })
        
        # 데이터 저장
        loader.load(self.test_data)
        
        # 디렉토리와 파일이 생성되었는지 확인
        self.assertTrue(os.path.exists(nested_path)) 
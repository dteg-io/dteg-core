"""
CSV Extractor 단위 테스트
"""
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from dteg.extractors.csv import CSVExtractor


class TestCSVExtractor(unittest.TestCase):
    """CSV Extractor 테스트"""

    def setUp(self):
        """테스트용 CSV 파일 생성"""
        # 임시 디렉토리 생성
        self.temp_dir = tempfile.TemporaryDirectory()
        
        # 테스트 CSV 파일 생성
        self.test_file1 = os.path.join(self.temp_dir.name, "test1.csv")
        self.test_file2 = os.path.join(self.temp_dir.name, "test2.csv")
        
        # 테스트 데이터 작성
        with open(self.test_file1, "w", encoding="utf-8") as f:
            f.write("id,name,value\n")
            f.write("1,Test 1,100\n")
            f.write("2,Test 2,200\n")
            
        with open(self.test_file2, "w", encoding="utf-8") as f:
            f.write("id,name,value\n")
            f.write("3,Test 3,300\n")
            f.write("4,Test 4,400\n")

    def tearDown(self):
        """테스트 종료 후 임시 파일 정리"""
        self.temp_dir.cleanup()

    def test_validate_config_required_fields(self):
        """필수 필드 검증 테스트"""
        # file_path 누락
        with self.assertRaises(ValueError):
            CSVExtractor({})

    def test_setup_file_not_found(self):
        """존재하지 않는 파일 검증"""
        # 존재하지 않는 파일 패턴
        with self.assertRaises(ValueError):
            extractor = CSVExtractor({
                "file_path": "/non/existent/path/*.csv"
            })
            extractor._setup()

    def test_extract_single_file(self):
        """단일 파일 추출 테스트"""
        extractor = CSVExtractor({
            "file_path": self.test_file1
        })
        
        result = extractor.extract()
        
        # 검증
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)  # 2개 행 검증
        self.assertEqual(result.iloc[0]["name"], "Test 1")
        self.assertEqual(result.iloc[1]["value"], 200)

    def test_extract_multiple_files(self):
        """다중 파일 추출 테스트"""
        # 와일드카드 패턴 사용
        extractor = CSVExtractor({
            "file_path": os.path.join(self.temp_dir.name, "*.csv")
        })
        
        result = extractor.extract()
        
        # 검증
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 4)  # 총 4개 행 검증
        self.assertEqual(result.iloc[2]["name"], "Test 3")
        self.assertEqual(result.iloc[3]["value"], 400)

    def test_extract_with_options(self):
        """다양한 옵션 테스트"""
        extractor = CSVExtractor({
            "file_path": self.test_file1,
            "delimiter": ",",
            "encoding": "utf-8",
            "dtype": {"id": int, "value": float},
            "usecols": ["id", "value"]
        })
        
        result = extractor.extract()
        
        # 검증
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result.columns), 2)  # id, value 컬럼만 있는지 확인
        self.assertNotIn("name", result.columns)
        self.assertEqual(result.iloc[0]["value"], 100.0)
        self.assertIsInstance(result.iloc[0]["value"], float)  # 타입 변환 검증

    def test_extract_batch(self):
        """배치 추출 테스트"""
        # 작은 배치 크기로 설정
        extractor = CSVExtractor({
            "file_path": os.path.join(self.temp_dir.name, "*.csv"),
            "batch_size": 1  # 한 번에 1개 행씩 배치 처리
        })
        
        batches = list(extractor.extract_batch())
        
        # 검증
        self.assertEqual(len(batches), 4)  # 총 4개 배치 (각 1행씩)
        self.assertEqual(len(batches[0]), 1)
        self.assertEqual(batches[0].iloc[0]["name"], "Test 1")
        self.assertEqual(batches[3].iloc[0]["name"], "Test 4")

    def test_extract_sample(self):
        """샘플 데이터 추출 테스트"""
        extractor = CSVExtractor({
            "file_path": os.path.join(self.temp_dir.name, "*.csv")
        })
        
        result = extractor.extract_sample(1)  # 1개 행만 샘플링
        
        # 검증
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1)  # 1개 행 검증
        self.assertEqual(result.iloc[0]["name"], "Test 1") 
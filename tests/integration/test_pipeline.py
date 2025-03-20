"""
파이프라인 통합 테스트
"""
import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest
import yaml

from dteg.core.context import ExecutionStatus
from dteg.core.pipeline import Pipeline
from dteg.utils.logging import configure_logging


# 테스트 시작 전 로깅 설정
configure_logging(level="INFO")


def create_test_config(
    source_type: str = "mysql",
    destination_type: str = "csv"
) -> str:
    """테스트용 설정 파일 생성
    
    Args:
        source_type: 소스 유형
        destination_type: 대상 유형
        
    Returns:
        생성된 설정 파일 경로
    """
    # 임시 디렉토리 생성
    temp_dir = tempfile.mkdtemp()
    
    # 설정 생성
    config = {
        "version": 1,
        "pipeline": {
            "name": "test-pipeline",
            "description": "테스트용 파이프라인",
            "source": {
                "type": source_type,
                "config": {}
            },
            "destination": {
                "type": destination_type,
                "config": {}
            },
            "variables": {},
            "logging": {
                "level": "DEBUG"
            }
        }
    }
    
    # source_type에 따른 설정 추가
    if source_type == "mysql":
        config["pipeline"]["source"]["config"] = {
            "host": "localhost",
            "port": 3306,
            "database": "test_db",
            "user": "test_user",
            "password": "test_password",
            "query": "SELECT * FROM test_table LIMIT 10"
        }
    elif source_type == "csv":
        # 테스트용 CSV 파일 생성
        csv_path = os.path.join(temp_dir, "source.csv")
        pd.DataFrame({
            "id": range(1, 6),
            "name": [f"Name {i}" for i in range(1, 6)],
            "value": [i * 10 for i in range(1, 6)]
        }).to_csv(csv_path, index=False)
        
        config["pipeline"]["source"]["config"] = {
            "file_path": csv_path,
            "delimiter": ","
        }
    
    # destination_type에 따른 설정 추가
    if destination_type == "csv":
        csv_path = os.path.join(temp_dir, "destination.csv")
        config["pipeline"]["destination"]["config"] = {
            "file_path": csv_path,
            "delimiter": ",",
            "if_exists": "append"
        }
    elif destination_type == "mysql":
        config["pipeline"]["destination"]["config"] = {
            "host": "localhost",
            "port": 3306,
            "database": "test_db",
            "user": "test_user",
            "password": "test_password",
            "table": "test_destination",
            "if_exists": "append"
        }
    
    # 설정 파일 생성
    config_path = os.path.join(temp_dir, "test_config.yaml")
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f)
    
    return config_path


@pytest.mark.skip(reason="외부 의존성 필요 (MySQL)")
def test_mysql_to_csv_pipeline():
    """MySQL에서 CSV로 데이터 파이프라인 테스트"""
    config_path = create_test_config("mysql", "csv")
    
    # 파이프라인 인스턴스 생성 및 실행
    pipeline = Pipeline(config_path)
    status, context = pipeline.run()
    
    # 결과 검증
    assert status == ExecutionStatus.SUCCEEDED
    assert context.metrics.rows_processed == 10  # 추출 5행 + 적재 5행 = 10행
    
    # 출력 파일 확인
    destination_path = pipeline.pipeline_config.destination.config["file_path"]
    assert os.path.exists(destination_path)
    
    # 출력 데이터 검증
    df = pd.read_csv(destination_path)
    assert len(df) == 10


def test_csv_to_csv_pipeline():
    """CSV에서 CSV로 데이터 파이프라인 테스트"""
    config_path = create_test_config("csv", "csv")
    
    # 파이프라인 인스턴스 생성 및 실행
    pipeline = Pipeline(config_path)
    status, context = pipeline.run()
    
    # 결과 검증
    assert status == ExecutionStatus.SUCCEEDED
    assert context.metrics.rows_processed == 10  # 추출 5행 + 적재 5행 = 10행
    
    # 출력 파일 확인
    destination_path = pipeline.pipeline_config.destination.config["file_path"]
    assert os.path.exists(destination_path)
    
    # 출력 데이터 검증
    df = pd.read_csv(destination_path)
    assert len(df) == 5
    assert list(df.columns) == ["id", "name", "value"]
    assert df["id"].tolist() == [1, 2, 3, 4, 5]


def test_pipeline_validation():
    """파이프라인 유효성 검사 테스트"""
    config_path = create_test_config("csv", "csv")
    
    # 파이프라인 인스턴스 생성 및 실행
    pipeline = Pipeline(config_path)
    is_valid = pipeline.validate()
    
    # 결과 검증
    assert is_valid is True


def test_batch_pipeline():
    """배치 모드 파이프라인 테스트"""
    config_path = create_test_config("csv", "csv")
    
    # 파이프라인 인스턴스 생성 및 실행
    pipeline = Pipeline(config_path)
    status, context = pipeline.run_batch(batch_size=2)
    
    # 결과 검증
    assert status == ExecutionStatus.SUCCEEDED
    assert context.metrics.rows_processed == 5  # 총 5개 행
    
    # 배치 정보 확인
    total_batches = context.get_variable("total_batches")
    assert total_batches == 3  # 2개씩 나누면 3배치 (2, 2, 1)
    
    # 출력 파일 확인
    destination_path = pipeline.pipeline_config.destination.config["file_path"]
    assert os.path.exists(destination_path)
    
    # 출력 데이터 검증
    df = pd.read_csv(destination_path)
    assert len(df) == 5  # 총 5개 행 (원본 데이터 그대로) 
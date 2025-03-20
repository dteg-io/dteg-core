#!/usr/bin/env python3
"""
테스트용 샘플 데이터 생성 스크립트
"""
import os
import sys

# 프로젝트 루트 디렉토리를 sys.path에 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.dteg.utils.samples import generate_sample_csv

# 샘플 CSV 파일 생성
generate_sample_csv(
    file_path="data/sample_data.csv",
    rows=100,
    columns={
        "id": "int",
        "name": "str",
        "value": "float",
        "created_at": "datetime",
        "is_active": "bool"
    },
    seed=42
)

print("샘플 데이터가 성공적으로 생성되었습니다.") 
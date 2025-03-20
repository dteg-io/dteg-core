"""
샘플 데이터 생성 유틸리티
"""
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
import numpy as np


def generate_sample_data(
    rows: int = 100,
    columns: Optional[Dict[str, str]] = None,
    seed: Optional[int] = None
) -> pd.DataFrame:
    """샘플 데이터를 생성합니다.
    
    Args:
        rows: 생성할 행 수
        columns: 컬럼 정의 (이름: 타입), 지원 타입: int, float, str, date, datetime, bool
        seed: 랜덤 시드
        
    Returns:
        샘플 데이터가 포함된 DataFrame
    """
    if seed is not None:
        random.seed(seed)
        np.random.seed(seed)
    
    # 기본 컬럼 정의
    if columns is None:
        columns = {
            "id": "int",
            "name": "str",
            "value": "float",
            "created_at": "datetime",
            "is_active": "bool"
        }
    
    data = {}
    
    for col_name, col_type in columns.items():
        if col_type == "int":
            data[col_name] = np.random.randint(1, 1000, size=rows)
        elif col_type == "float":
            data[col_name] = np.random.uniform(0, 100, size=rows).round(2)
        elif col_type == "str":
            data[col_name] = [f"Item {i}" for i in range(1, rows + 1)]
        elif col_type == "date":
            start_date = datetime.now().date() - timedelta(days=365)
            data[col_name] = [
                (start_date + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
                for _ in range(rows)
            ]
        elif col_type == "datetime":
            start_date = datetime.now() - timedelta(days=30)
            data[col_name] = [
                (start_date + timedelta(
                    days=random.randint(0, 30),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59)
                )).strftime("%Y-%m-%d %H:%M:%S")
                for _ in range(rows)
            ]
        elif col_type == "bool":
            data[col_name] = np.random.choice([True, False], size=rows)
        else:
            # 기본값은 문자열
            data[col_name] = [f"Value {i}" for i in range(1, rows + 1)]
    
    return pd.DataFrame(data)


def generate_sample_csv(
    file_path: str,
    rows: int = 100,
    columns: Optional[Dict[str, str]] = None,
    seed: Optional[int] = None,
    **csv_kwargs
) -> None:
    """샘플 CSV 파일을 생성합니다.
    
    Args:
        file_path: 생성할 CSV 파일 경로
        rows: 생성할 행 수
        columns: 컬럼 정의 (이름: 타입)
        seed: 랜덤 시드
        **csv_kwargs: to_csv 함수에 전달할 추가 인자
    """
    # 기본 CSV 옵션
    csv_options = {
        "index": False,
        "encoding": "utf-8"
    }
    csv_options.update(csv_kwargs)
    
    # 샘플 데이터 생성
    df = generate_sample_data(rows=rows, columns=columns, seed=seed)
    
    # CSV 파일로 저장
    df.to_csv(file_path, **csv_options)


def generate_time_series_data(
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
    freq: str = "D",
    value_column: str = "value",
    value_range: Tuple[float, float] = (0, 100),
    trend: float = 0.0,
    seasonality: bool = False,
    noise_level: float = 0.1,
    seed: Optional[int] = None
) -> pd.DataFrame:
    """시계열 데이터를 생성합니다.
    
    Args:
        start_date: 시작 날짜
        end_date: 종료 날짜
        freq: 빈도 (D: 일, H: 시간, T: 분, S: 초)
        value_column: 값 컬럼명
        value_range: 값 범위 (최소, 최대)
        trend: 추세 강도 (양수: 상승, 음수: 하락)
        seasonality: 계절성 추가 여부
        noise_level: 노이즈 수준 (0.0 ~ 1.0)
        seed: 랜덤 시드
    
    Returns:
        시계열 데이터가 포함된 DataFrame
    """
    if seed is not None:
        np.random.seed(seed)
    
    # 날짜 범위 생성
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)
    
    # 날짜 인덱스 생성
    date_range = pd.date_range(start=start_date, end=end_date, freq=freq)
    
    # 기본 데이터프레임 생성
    df = pd.DataFrame(index=date_range)
    
    # 시간 인덱스 (0부터 시작)
    t = np.arange(len(df))
    
    # 트렌드 생성
    trend_component = trend * t / len(t)
    
    # 계절성 생성 (있는 경우)
    if seasonality:
        # 하루 주기 또는 1년 주기에 따라 계절성 적용
        if freq in ["H", "T", "S"]:
            # 하루 주기 (24시간)
            period = 24 if freq == "H" else 24 * 60 if freq == "T" else 24 * 60 * 60
            seasonality_component = 0.5 * np.sin(2 * np.pi * t / period)
        else:
            # 1년 주기 (365일)
            period = 365 if freq == "D" else 12
            seasonality_component = 0.5 * np.sin(2 * np.pi * t / period)
    else:
        seasonality_component = 0
    
    # 노이즈 생성
    noise = noise_level * np.random.normal(0, 1, len(t))
    
    # 각 컴포넌트 결합
    min_val, max_val = value_range
    scale = max_val - min_val
    base = min_val
    
    values = base + scale * (0.5 + 0.5 * (trend_component + seasonality_component + noise))
    
    # 데이터프레임에 값 추가
    df[value_column] = values
    
    # datetime 인덱스를 컬럼으로 변환
    df.reset_index(inplace=True)
    df.rename(columns={"index": "date"}, inplace=True)
    
    return df 
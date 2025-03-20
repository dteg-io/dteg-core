"""
SQLTransformer 단위 테스트
"""
import pandas as pd
import pytest

from dteg.transformers.sql import SQLTransformer


def test_sql_transformer_basic():
    """SQLTransformer 기본 변환 테스트"""
    # 테스트 데이터 생성
    data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eva'],
        'age': [25, 30, 35, 40, 45],
        'active': [True, False, True, True, False]
    })
    
    # SQLTransformer 인스턴스 생성
    config = {
        'engine': 'sqlite',
        'temp_table': 'test_data',
        'query': 'SELECT * FROM test_data WHERE age > 30'
    }
    transformer = SQLTransformer(config)
    
    # 변환 실행
    result = transformer.transform(data)
    
    # 결과 검증
    assert len(result) == 3
    assert list(result['name']) == ['Charlie', 'David', 'Eva']
    assert list(result['age']) == [35, 40, 45]


def test_sql_transformer_aggregation():
    """SQLTransformer 집계 기능 테스트"""
    # 테스트 데이터 생성
    data = pd.DataFrame({
        'category': ['A', 'B', 'A', 'C', 'B', 'A'],
        'value': [10, 20, 15, 30, 25, 5]
    })
    
    # SQLTransformer 인스턴스 생성
    config = {
        'engine': 'sqlite',
        'temp_table': 'test_data',
        'query': '''
            SELECT 
                category, 
                SUM(value) as total, 
                AVG(value) as average, 
                COUNT(*) as count 
            FROM test_data 
            GROUP BY category
            ORDER BY category
        '''
    }
    transformer = SQLTransformer(config)
    
    # 변환 실행
    result = transformer.transform(data)
    
    # 결과 검증
    assert len(result) == 3
    assert list(result['category']) == ['A', 'B', 'C']
    assert list(result['total']) == [30, 45, 30]
    assert list(result['count']) == [3, 2, 1]


def test_sql_transformer_template():
    """SQLTransformer 템플릿 기능 테스트"""
    # 테스트 데이터 생성
    data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'value': [10, 20, 30, 40, 50],
        'category': ['A', 'B', 'A', 'C', 'B']
    })
    
    # SQLTransformer 인스턴스 생성 (템플릿 파라미터 포함)
    config = {
        'engine': 'sqlite',
        'temp_table': 'test_data',
        'query': 'SELECT * FROM test_data WHERE category = "{{ category }}" AND value > {{ min_value }}',
        'query_params': {
            'category': 'B',
            'min_value': 20
        }
    }
    transformer = SQLTransformer(config)
    
    # 변환 실행
    result = transformer.transform(data)
    
    # 결과 검증
    assert len(result) == 1
    assert result.iloc[0]['id'] == 5
    assert result.iloc[0]['value'] == 50


def test_sql_transformer_invalid_query():
    """SQLTransformer 잘못된 쿼리 처리 테스트"""
    # 테스트 데이터 생성
    data = pd.DataFrame({'id': [1, 2, 3]})
    
    # 잘못된 쿼리로 SQLTransformer 인스턴스 생성
    config = {
        'engine': 'sqlite',
        'temp_table': 'test_data',
        'query': 'SELECT * FROM non_existent_table'
    }
    transformer = SQLTransformer(config)
    
    # 예외 발생 테스트
    with pytest.raises(Exception):
        transformer.transform(data)


def test_sql_transformer_pandas_engine():
    """SQLTransformer Pandas 엔진 테스트"""
    # 테스트 데이터 생성
    data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'value': [10, 20, 30, 40, 50]
    })
    
    # Pandas 엔진 SQLTransformer 인스턴스 생성
    config = {
        'engine': 'pandas',
        'query': 'SELECT * FROM table WHERE value > 25'
    }
    transformer = SQLTransformer(config)
    
    # 변환 실행
    result = transformer.transform(data)
    
    # 결과 검증
    assert len(result) == 3
    assert list(result['id']) == [3, 4, 5]
    assert list(result['value']) == [30, 40, 50] 
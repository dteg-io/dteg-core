"""
YAML 설정 파일 처리 모듈
"""
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from pydantic import BaseModel, Field, ValidationError


class ConfigValidationError(Exception):
    """설정 파일 검증 오류"""
    pass


class VariableNotFoundError(Exception):
    """변수를 찾을 수 없음"""
    pass


class SourceConfig(BaseModel):
    """데이터 소스 설정"""
    type: str
    config: Dict[str, Any] = Field(default_factory=dict)


class DestinationConfig(BaseModel):
    """데이터 대상 설정"""
    type: str
    config: Dict[str, Any] = Field(default_factory=dict)


class LoggingConfig(BaseModel):
    """로깅 설정"""
    level: str = "INFO"
    file: Optional[str] = None
    directory: Optional[str] = None


class TransformerConfig(BaseModel):
    """변환기 설정"""
    type: str = "sql"
    config: Dict[str, Any] = Field(default_factory=dict)


class PipelineStepConfig(BaseModel):
    """파이프라인 단계 설정"""
    name: str
    description: Optional[str] = None
    retry: Dict[str, Any] = Field(default_factory=lambda: {"max_retries": 3, "delay_seconds": 5})
    timeout: Optional[int] = 3600  # 기본 1시간 타임아웃


class PipelineConfig(BaseModel):
    """파이프라인 설정"""
    name: str
    description: Optional[str] = None
    pipeline_id: Optional[str] = None
    source: SourceConfig
    destination: DestinationConfig
    transformer: Optional[TransformerConfig] = None
    variables: Dict[str, Any] = Field(default_factory=dict)
    steps: List[PipelineStepConfig] = Field(default_factory=list)
    schedule: Optional[str] = None
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    @classmethod
    def from_yaml(cls, yaml_path: Union[str, Path]) -> 'PipelineConfig':
        """YAML 파일에서 파이프라인 설정 로드
        
        Args:
            yaml_path: YAML 파일 경로
            
        Returns:
            PipelineConfig 객체
            
        Raises:
            FileNotFoundError: 파일을 찾을 수 없는 경우
            yaml.YAMLError: YAML 파싱 오류
            ConfigValidationError: 설정 유효성 검증 실패
        """
        config = load_config(str(yaml_path))
        pipeline_config = config.pipeline
        
        # pipeline_id가 설정되어 있지 않으면 파일 경로를 pipeline_id로 사용
        if not pipeline_config.pipeline_id:
            pipeline_config.pipeline_id = str(yaml_path)
        
        return pipeline_config


class Config(BaseModel):
    """전체 설정 파일 스키마"""
    version: int = 1
    pipeline: PipelineConfig


def _expand_env_vars(config_dict: Dict[str, Any]) -> Dict[str, Any]:
    """설정 딕셔너리 내 환경 변수 확장"""
    if not isinstance(config_dict, dict):
        return config_dict

    result = {}
    for key, value in config_dict.items():
        if isinstance(value, dict):
            result[key] = _expand_env_vars(value)
        elif isinstance(value, list):
            result[key] = [_expand_env_vars(item) if isinstance(item, dict) else item for item in value]
        elif isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            env_var = value[2:-1]
            result[key] = os.environ.get(env_var, value)
        else:
            result[key] = value
    return result


def _resolve_variables(config_dict: Dict[str, Any], variables: Dict[str, Any]) -> Dict[str, Any]:
    """설정 딕셔너리 내 변수 해석

    Args:
        config_dict: 설정 딕셔너리
        variables: 변수 딕셔너리

    Returns:
        변수가 해석된 설정 딕셔너리
    """
    if not isinstance(config_dict, dict):
        return config_dict

    result = {}
    for key, value in config_dict.items():
        if isinstance(value, dict):
            result[key] = _resolve_variables(value, variables)
        elif isinstance(value, list):
            result[key] = [
                _resolve_variables(item, variables) if isinstance(item, dict) 
                else _resolve_string_variables(item, variables) if isinstance(item, str)
                else item 
                for item in value
            ]
        elif isinstance(value, str):
            result[key] = _resolve_string_variables(value, variables)
        else:
            result[key] = value
    return result


def _resolve_string_variables(text: str, variables: Dict[str, Any]) -> str:
    """문자열 내 변수 해석

    Args:
        text: 변수를 포함할 수 있는 문자열
        variables: 변수 딕셔너리

    Returns:
        변수가 해석된 문자열
    """
    if not isinstance(text, str):
        return text

    # 정규 표현식으로 변수 패턴 ({{ variable }}) 찾기
    pattern = r"\{\{\s*([a-zA-Z0-9_\.]+)\s*\}\}"
    
    def replace_var(match):
        var_name = match.group(1).strip()
        
        # 중첩 변수 처리 (예: user.name)
        parts = var_name.split('.')
        value = variables
        
        try:
            for part in parts:
                if isinstance(value, dict) and part in value:
                    value = value[part]
                else:
                    raise VariableNotFoundError(f"변수를 찾을 수 없음: {var_name}")
                    
            # 변수 타입에 따른 처리
            if isinstance(value, (int, float, bool)):
                return str(value)
            elif isinstance(value, str):
                return value
            elif isinstance(value, (dict, list)):
                return yaml.dump(value)
            else:
                return str(value)
                
        except (KeyError, TypeError):
            raise VariableNotFoundError(f"변수를 찾을 수 없음: {var_name}")
    
    # 변수 대체
    try:
        return re.sub(pattern, replace_var, text)
    except VariableNotFoundError as e:
        # 변수를 찾을 수 없는 경우 원본 반환
        return text


def load_config(config_path: str, runtime_variables: Optional[Dict[str, Any]] = None) -> Config:
    """YAML 설정 파일 로드 및 검증

    Args:
        config_path: YAML 설정 파일 경로
        runtime_variables: 실행 시 추가할 변수 (기본값: None)

    Returns:
        검증된 설정 객체

    Raises:
        FileNotFoundError: 설정 파일을 찾을 수 없는 경우
        yaml.YAMLError: YAML 파싱 오류
        ConfigValidationError: 스키마 검증 실패
    """
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config_dict = yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"설정 파일을 찾을 수 없습니다: {config_path}")
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"YAML 파싱 오류: {e}")

    # 환경 변수 확장
    config_dict = _expand_env_vars(config_dict)

    # 기본 변수 설정
    variables = {
        "datetime": {
            "now": datetime.now().isoformat(),
            "date": datetime.now().strftime("%Y-%m-%d"),
            "time": datetime.now().strftime("%H:%M:%S"),
            "year": datetime.now().year,
            "month": datetime.now().month,
            "day": datetime.now().day,
        },
        "env": dict(os.environ),
    }

    # 설정 내 정의된 변수 추가
    if "pipeline" in config_dict and "variables" in config_dict["pipeline"]:
        variables.update(config_dict["pipeline"]["variables"])

    # 런타임 변수 추가
    if runtime_variables:
        variables.update(runtime_variables)

    # 변수 해석
    config_dict = _resolve_variables(config_dict, variables)

    try:
        # 스키마 검증
        return Config(**config_dict)
    except ValidationError as e:
        raise ConfigValidationError(f"설정 파일 검증 실패: {e}")


def generate_default_config() -> Dict[str, Any]:
    """기본 설정 생성
    
    Returns:
        기본 설정 딕셔너리
    """
    return {
        "version": 1,
        "pipeline": {
            "name": "default-pipeline",
            "description": "기본 파이프라인 설정",
            "source": {
                "type": "mysql",
                "config": {
                    "host": "localhost",
                    "port": 3306,
                    "database": "test",
                    "user": "${MYSQL_USER}",
                    "password": "${MYSQL_PASSWORD}",
                    "table": "source_table"
                }
            },
            "destination": {
                "type": "csv",
                "config": {
                    "path": "output.csv",
                    "delimiter": ",",
                    "encoding": "utf-8"
                }
            },
            "variables": {
                "batch_size": 1000,
                "retry_count": 3
            },
            "logging": {
                "level": "INFO",
                "file": "pipeline.log"
            }
        }
    } 
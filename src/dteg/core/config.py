"""
YAML 설정 파일 처리 모듈
"""
import os
from typing import Any, Dict, Optional

import yaml
from pydantic import BaseModel, Field, ValidationError


class SourceConfig(BaseModel):
    """데이터 소스 설정"""
    type: str
    config: Dict[str, Any] = Field(default_factory=dict)


class DestinationConfig(BaseModel):
    """데이터 대상 설정"""
    type: str
    config: Dict[str, Any] = Field(default_factory=dict)


class PipelineConfig(BaseModel):
    """파이프라인 설정"""
    name: str
    description: Optional[str] = None
    source: SourceConfig
    destination: DestinationConfig


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


def load_config(config_path: str) -> Config:
    """YAML 설정 파일 로드 및 검증

    Args:
        config_path: YAML 설정 파일 경로

    Returns:
        검증된 설정 객체

    Raises:
        FileNotFoundError: 설정 파일을 찾을 수 없는 경우
        yaml.YAMLError: YAML 파싱 오류
        ValidationError: 스키마 검증 실패
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

    try:
        # 스키마 검증
        return Config(**config_dict)
    except ValidationError as e:
        raise ValidationError(f"설정 파일 검증 실패: {e}", e.raw_errors) 
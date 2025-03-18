"""
설정 모듈 단위 테스트
"""
import os
import tempfile
from unittest import TestCase

import pytest
import yaml
from pydantic import ValidationError

from dteg.core.config import Config, load_config


class TestConfig(TestCase):
    """설정 파일 로드 및 검증 테스트"""

    def test_valid_config(self):
        """유효한 설정 파일이 올바르게 로드되는지 검증"""
        # 테스트용 설정 파일 생성
        config_dict = {
            "version": 1,
            "pipeline": {
                "name": "test-pipeline",
                "source": {
                    "type": "mysql",
                    "config": {
                        "host": "localhost",
                        "database": "test_db",
                        "user": "test_user",
                        "password": "test_password",
                    },
                },
                "destination": {
                    "type": "bigquery",
                    "config": {
                        "project": "test-project",
                        "dataset": "test_dataset",
                        "table": "test_table",
                    },
                },
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_dict, f)
            config_path = f.name

        try:
            # 설정 파일 로드
            config = load_config(config_path)

            # 검증
            self.assertEqual(config.version, 1)
            self.assertEqual(config.pipeline.name, "test-pipeline")
            self.assertEqual(config.pipeline.source.type, "mysql")
            self.assertEqual(config.pipeline.source.config["host"], "localhost")
            self.assertEqual(config.pipeline.destination.type, "bigquery")
            self.assertEqual(config.pipeline.destination.config["project"], "test-project")
        finally:
            # 임시 파일 삭제
            os.unlink(config_path)

    def test_missing_required_fields(self):
        """필수 필드가 누락된 경우 오류가 발생하는지 검증"""
        # 필수 필드가 누락된 설정
        invalid_config = {"version": 1}  # pipeline 필드 누락

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(invalid_config, f)
            config_path = f.name

        try:
            # ValidationError가 발생해야 함
            with pytest.raises(ValidationError):
                load_config(config_path)
        finally:
            # 임시 파일 삭제
            os.unlink(config_path)

    def test_env_var_expansion(self):
        """환경 변수가 올바르게 확장되는지 검증"""
        # 테스트용 환경 변수 설정
        os.environ["TEST_DB_USER"] = "env_user"
        os.environ["TEST_DB_PASSWORD"] = "env_password"

        # 환경 변수를 포함한 설정
        config_dict = {
            "version": 1,
            "pipeline": {
                "name": "test-pipeline",
                "source": {
                    "type": "mysql",
                    "config": {
                        "host": "localhost",
                        "database": "test_db",
                        "user": "${TEST_DB_USER}",
                        "password": "${TEST_DB_PASSWORD}",
                    },
                },
                "destination": {
                    "type": "bigquery",
                    "config": {
                        "project": "test-project",
                        "dataset": "test_dataset",
                        "table": "test_table",
                    },
                },
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_dict, f)
            config_path = f.name

        try:
            # 설정 파일 로드
            config = load_config(config_path)

            # 환경 변수가 확장되었는지 검증
            self.assertEqual(config.pipeline.source.config["user"], "env_user")
            self.assertEqual(config.pipeline.source.config["password"], "env_password")
        finally:
            # 임시 파일 삭제
            os.unlink(config_path) 
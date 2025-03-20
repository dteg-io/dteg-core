"""
dbt 프로젝트와 연동하는 Transformer 구현
"""
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

from dteg.transformers.base import BaseTransformer
from dteg.utils.logging import get_logger

# 로거 초기화
logger = get_logger()


class DbtTransformer(BaseTransformer):
    """dbt 프로젝트를 실행하여 데이터를 변환하는 Transformer"""

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        DbtTransformer 초기화

        Args:
            config: 설정 딕셔너리
                - project_dir: dbt 프로젝트 디렉토리 경로
                - profiles_dir: dbt profiles.yml 파일 디렉토리 (선택, 기본값: ~/.dbt)
                - target: dbt target 프로필 (선택)
                - models: 실행할 모델 목록이나 선택기 (선택)
                - vars: dbt에 전달할 변수 (선택)
                - full_refresh: 전체 새로고침 여부 (선택, 기본값: False)
                - result_source: 결과를 가져올 소스 유형 ('table', 'csv', 'json' 중 하나, 기본값: 'table')
                - result_path: 결과 테이블/파일 경로 (result_source에 따라 다름)
        """
        super().__init__(config)

    def initialize(self) -> None:
        """추가 초기화 작업 수행"""
        # 필수 설정 확인
        if "project_dir" not in self.config:
            raise ValueError("DBT Transformer에 'project_dir' 설정이 필요합니다")
            
        if "result_path" not in self.config:
            raise ValueError("DBT Transformer에 'result_path' 설정이 필요합니다")
            
        # 기본값 설정
        self.project_dir = os.path.expanduser(self.config["project_dir"])
        self.profiles_dir = os.path.expanduser(self.config.get("profiles_dir", "~/.dbt"))
        self.target = self.config.get("target")
        self.models = self.config.get("models", [])
        self.vars = self.config.get("vars", {})
        self.full_refresh = self.config.get("full_refresh", False)
        self.result_source = self.config.get("result_source", "table")
        self.result_path = self.config["result_path"]
        
        # dbt 프로젝트 디렉토리 존재 여부 확인
        if not os.path.exists(self.project_dir):
            raise FileNotFoundError(f"dbt 프로젝트 디렉토리를 찾을 수 없습니다: {self.project_dir}")
            
        # dbt 명령어 존재 여부 확인
        try:
            subprocess.run(["dbt", "--version"], 
                        check=True, 
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE)
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise RuntimeError("dbt가 설치되어 있지 않거나 실행할 수 없습니다. `pip install dbt-core`로 설치하세요.")

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        dbt 모델을 실행하여 데이터 변환

        Args:
            data: 변환할 원본 데이터 (dbt 실행에는 사용되지 않지만 인터페이스 일관성을 위해 필요)

        Returns:
            변환된 데이터
        """
        # dbt를 실행한 후 결과 반환
        self._run_dbt()
        return self._get_results()

    def _run_dbt(self) -> None:
        """
        dbt 명령 실행
        """
        cmd = ["dbt", "run", "--project-dir", self.project_dir, "--profiles-dir", self.profiles_dir]
        
        # 선택적 인자 추가
        if self.target:
            cmd.extend(["--target", self.target])
            
        if self.models:
            if isinstance(self.models, list):
                models_arg = " ".join(self.models)
            else:
                models_arg = self.models
            cmd.extend(["--models", models_arg])
            
        if self.vars:
            vars_json = json.dumps(self.vars)
            cmd.extend(["--vars", vars_json])
            
        if self.full_refresh:
            cmd.append("--full-refresh")
            
        # 명령 실행
        logger.info(f"dbt 명령 실행: {' '.join(cmd)}")
        try:
            result = subprocess.run(
                cmd,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=self.project_dir
            )
            logger.debug(f"dbt 실행 결과:\n{result.stdout}")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"dbt 실행 실패 (종료 코드: {e.returncode}):\n{e.stderr}")
            raise RuntimeError(f"dbt 실행 실패: {e.stderr}")

    def _run_dbt_test(self) -> bool:
        """
        dbt 테스트 실행

        Returns:
            테스트 성공 여부
        """
        cmd = ["dbt", "test", "--project-dir", self.project_dir, "--profiles-dir", self.profiles_dir]
        
        # 선택적 인자 추가
        if self.target:
            cmd.extend(["--target", self.target])
            
        if self.models:
            if isinstance(self.models, list):
                models_arg = " ".join(self.models)
            else:
                models_arg = self.models
            cmd.extend(["--models", models_arg])
            
        # 명령 실행
        logger.info(f"dbt 테스트 실행: {' '.join(cmd)}")
        try:
            result = subprocess.run(
                cmd,
                check=False,  # 테스트 실패해도 예외 발생하지 않도록
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=self.project_dir
            )
            
            if result.returncode == 0:
                logger.info("dbt 테스트 성공")
                return True
            else:
                logger.warning(f"dbt 테스트 실패:\n{result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"dbt 테스트 실행 중 오류 발생: {str(e)}")
            return False

    def _get_results(self) -> pd.DataFrame:
        """
        dbt 실행 결과 가져오기

        Returns:
            결과 데이터프레임
        """
        if self.result_source == "table":
            return self._get_results_from_table()
        elif self.result_source == "csv":
            return self._get_results_from_csv()
        elif self.result_source == "json":
            return self._get_results_from_json()
        else:
            raise ValueError(f"지원하지 않는 결과 소스 유형: {self.result_source}")

    def _get_results_from_table(self) -> pd.DataFrame:
        """
        데이터베이스 테이블에서 결과 가져오기
        
        Returns:
            결과 데이터프레임
        """
        # 테이블 결과는 별도의 SQL Extractor를 사용하여 가져와야 함
        # 여기서는 간단한 처리만 하고 실제 구현은 사용 시 개발 필요
        logger.warning("테이블에서 결과를 가져오는 기능은 별도의 Extractor를 사용해야 합니다.")
        return pd.DataFrame()

    def _get_results_from_csv(self) -> pd.DataFrame:
        """
        CSV 파일에서 결과 가져오기
        
        Returns:
            결과 데이터프레임
        """
        try:
            path = os.path.expanduser(self.result_path)
            if not os.path.exists(path):
                logger.error(f"CSV 결과 파일을 찾을 수 없습니다: {path}")
                return pd.DataFrame()
                
            return pd.read_csv(path)
        except Exception as e:
            logger.error(f"CSV 결과 파일 읽기 실패: {str(e)}")
            return pd.DataFrame()

    def _get_results_from_json(self) -> pd.DataFrame:
        """
        JSON 파일에서 결과 가져오기
        
        Returns:
            결과 데이터프레임
        """
        try:
            path = os.path.expanduser(self.result_path)
            if not os.path.exists(path):
                logger.error(f"JSON 결과 파일을 찾을 수 없습니다: {path}")
                return pd.DataFrame()
                
            return pd.read_json(path)
        except Exception as e:
            logger.error(f"JSON 결과 파일 읽기 실패: {str(e)}")
            return pd.DataFrame()

    def get_metadata(self) -> Dict[str, Any]:
        """
        Transformer 메타데이터 반환
        
        Returns:
            메타데이터 딕셔너리
        """
        metadata = super().get_metadata()
        # dbt 프로젝트 정보 추가
        metadata.update({
            "dbt_project": self.project_dir,
            "models": self.models,
            "result_source": self.result_source,
            "result_path": self.result_path
        })
        return metadata
        
    def close(self) -> None:
        """리소스 정리"""
        # dbt는 특별히 정리할 자원이 없음
        pass 
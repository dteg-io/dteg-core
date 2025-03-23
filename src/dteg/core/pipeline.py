"""
파이프라인 실행 엔진 구현 모듈
"""
import os
import time
import traceback
from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional, Tuple, Union

import pandas as pd

from dteg.core.config import Config, PipelineConfig
from dteg.core.context import ExecutionStatus, PipelineContext
from dteg.core.plugin import create_extractor, create_loader, create_transformer, discover_plugins
from dteg.utils.logging import get_logger

# 로거 가져오기
logger = get_logger()


class Pipeline:
    """데이터 파이프라인 메인 클래스"""

    def __init__(self, config: Union[Config, Dict[str, Any], str]) -> None:
        """
        Args:
            config: 파이프라인 설정 객체, 딕셔너리 또는 설정 파일 경로
        """
        # 설정 로드
        if isinstance(config, str):
            from dteg.core.config import load_config
            loaded_config = load_config(config)
            self.config = loaded_config
            # 파이프라인 설정 분리
            self.pipeline_config = loaded_config.get_pipeline_config()
        elif isinstance(config, dict):
            from dteg.core.config import Config
            self.config = Config(**config)
            # 파이프라인 설정 분리
            self.pipeline_config = self.config.get_pipeline_config()
        elif hasattr(config, 'get_pipeline_config'):
            # Config 객체인 경우
            self.config = config
            self.pipeline_config = config.get_pipeline_config()
        else:
            # 이미 PipelineConfig 객체인 경우
            self.config = config
            self.pipeline_config = config
        
        # 컨텍스트 초기화
        self.context = PipelineContext(
            pipeline_name=self.pipeline_config.name,
            config=self.pipeline_config.model_dump()
        )
        
        # 플러그인 검색
        discover_plugins()
        
        # 컴포넌트 초기화
        self._extractor = None
        self._transformer = None
        self._loader = None
    
    def _init_components(self) -> None:
        """컴포넌트 초기화"""
        # Extractor 인스턴스 생성
        source_config = self.pipeline_config.source
        
        # 모든 필드를 포함한 설정 가져오기
        source_settings = source_config.model_dump()
        
        # type 필드는 제외하고 설정 전달 (이미 create_extractor의 별도 인자로 전달됨)
        if 'type' in source_settings:
            del source_settings['type']
        
        # config 필드가 있으면 내용을 최상위로 병합하고 원래의 config 필드 제거
        if 'config' in source_settings and isinstance(source_settings['config'], dict):
            for key, value in source_settings['config'].items():
                if key != 'type' and key not in source_settings:
                    source_settings[key] = value
            del source_settings['config']
            
        self._extractor = create_extractor(
            source_config.type, source_settings
        )
        
        # Transformer 인스턴스 생성
        if self.pipeline_config.transformer:
            transformer_config = self.pipeline_config.transformer
            
            # 모든 필드를 포함한 설정 가져오기
            transformer_settings = transformer_config.model_dump()
            
            if 'type' in transformer_settings:
                del transformer_settings['type']
                
            # config 필드가 있으면 내용을 최상위로 병합하고 원래의 config 필드 제거
            if 'config' in transformer_settings and isinstance(transformer_settings['config'], dict):
                for key, value in transformer_settings['config'].items():
                    if key != 'type' and key not in transformer_settings:
                        transformer_settings[key] = value
                del transformer_settings['config']
                
            self._transformer = create_transformer(
                transformer_config.type, transformer_settings
            )
        
        # Loader 인스턴스 생성
        destination_config = self.pipeline_config.destination
        
        # 모든 필드를 포함한 설정 가져오기
        destination_settings = destination_config.model_dump()
        
        if 'type' in destination_settings:
            del destination_settings['type']
            
        # config 필드가 있으면 내용을 최상위로 병합하고 원래의 config 필드 제거
        if 'config' in destination_settings and isinstance(destination_settings['config'], dict):
            for key, value in destination_settings['config'].items():
                if key != 'type' and key not in destination_settings:
                    destination_settings[key] = value
            del destination_settings['config']
            
        self._loader = create_loader(
            destination_config.type, destination_settings
        )
        
        logger.debug(f"컴포넌트 초기화 완료: {source_config.type} → {destination_config.type}")
    
    def _extract_data(self) -> pd.DataFrame:
        """데이터 추출
        
        Returns:
            추출된 데이터
        """
        logger.info(f"데이터 추출 시작: {self.pipeline_config.source.type}")
        
        try:
            data = self._extractor.extract()
            
            # 컨텍스트 업데이트
            self.context.update_metrics(data=data)
            self.context.log_event(
                "extract",
                f"데이터 추출 완료 ({len(data)} 행)",
                {"source_type": self.pipeline_config.source.type}
            )
            
            logger.info(f"데이터 추출 완료: {len(data)} 행")
            return data
        except Exception as e:
            error_msg = f"데이터 추출 중 오류 발생: {str(e)}"
            logger.error(error_msg)
            self.context.log_event("error", error_msg, {"traceback": traceback.format_exc()})
            raise
    
    def _transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """데이터 변환
        
        Args:
            data: 원본 데이터
            
        Returns:
            변환된 데이터
        """
        # 변환기가 없으면 원본 반환
        if not self.pipeline_config.transformer:
            logger.debug("변환기가 설정되지 않음, 원본 데이터 사용")
            return data
        
        transformer_config = self.pipeline_config.transformer
        logger.info(f"데이터 변환 시작: {transformer_config.type}")
        
        try:
            # Transformer를 사용하여 데이터 변환
            transformed_data = self._transformer.transform(data)
            
            # 컨텍스트 업데이트
            self.context.update_metrics(data=transformed_data)
            self.context.log_event(
                "transform",
                f"데이터 변환 완료 ({len(transformed_data)} 행)",
                {"transformer_type": transformer_config.type}
            )
            
            logger.info(f"데이터 변환 완료: {len(transformed_data)} 행")
            return transformed_data
        except Exception as e:
            error_msg = f"데이터 변환 중 오류 발생: {str(e)}"
            logger.error(error_msg)
            self.context.log_event("error", error_msg, {"traceback": traceback.format_exc()})
            raise
    
    def _load_data(self, data: pd.DataFrame) -> int:
        """데이터 적재
        
        Args:
            data: 적재할 데이터
            
        Returns:
            적재된 행 수
        """
        logger.info(f"데이터 적재 시작: {self.pipeline_config.destination.type}")
        
        try:
            rows_loaded = self._loader.load(data)
            
            # 컨텍스트 업데이트
            self.context.update_metrics(rows=rows_loaded)
            self.context.log_event(
                "load",
                f"데이터 적재 완료 ({rows_loaded} 행)",
                {"destination_type": self.pipeline_config.destination.type}
            )
            
            logger.info(f"데이터 적재 완료: {rows_loaded} 행")
            return rows_loaded
        except Exception as e:
            error_msg = f"데이터 적재 중 오류 발생: {str(e)}"
            logger.error(error_msg)
            self.context.log_event("error", error_msg, {"traceback": traceback.format_exc()})
            raise
    
    def _close_components(self) -> None:
        """컴포넌트 자원 정리"""
        if self._extractor:
            try:
                self._extractor.close()
            except Exception as e:
                logger.warning(f"Extractor 종료 중 오류 발생: {e}")
        
        if self._transformer:
            try:
                self._transformer.close()
            except Exception as e:
                logger.warning(f"Transformer 종료 중 오류 발생: {e}")
        
        if self._loader:
            try:
                self._loader.close()
            except Exception as e:
                logger.warning(f"Loader 종료 중 오류 발생: {e}")
    
    @contextmanager
    def _pipeline_context(self) -> Generator[PipelineContext, None, None]:
        """파이프라인 컨텍스트 관리
        
        컨텍스트 매니저로 파이프라인 실행 환경을 제공하고
        예외 처리 및 리소스 정리를 자동화합니다.
        """
        # 컨텍스트 시작
        self.context.start()
        self.context.log_event("start", f"파이프라인 실행 시작: {self.pipeline_config.name}")
        
        try:
            # 컨텍스트 제공
            yield self.context
            
            # 정상 종료
            self.context.complete(success=True)
            self.context.log_event("complete", "파이프라인 실행 성공")
        except Exception as e:
            # 오류 발생 시 처리
            self.context.fail(e)
            self.context.log_event(
                "fail", 
                f"파이프라인 실행 실패: {str(e)}",
                {"traceback": traceback.format_exc()}
            )
            raise
        finally:
            # 리소스 정리
            try:
                self._close_components()
            except Exception as e:
                logger.error(f"리소스 정리 중 오류 발생: {e}")
            
            # 로깅 및 메트릭 요약
            execution_time = self.context.metrics.get_execution_time()
            if execution_time:
                logger.info(f"파이프라인 실행 시간: {execution_time:.2f}초")
            
            logger.info(f"파이프라인 상태: {self.context.status.value}")
            logger.info(f"처리된 행 수: {self.context.metrics.rows_processed}")
    
    def run(self) -> Tuple[ExecutionStatus, PipelineContext]:
        """파이프라인 실행
        
        Returns:
            실행 상태와 컨텍스트 정보
        """
        logger.info(f"파이프라인 시작: {self.pipeline_config.name}")
        
        # 컨텍스트 매니저를 통한 실행
        with self._pipeline_context() as context:
            try:
                # 컴포넌트 초기화
                self._init_components()
                
                # 메인 ETL 로직 실행
                data = self._extract_data()
                transformed_data = self._transform_data(data)
                self._load_data(transformed_data)
                
                # 결과 데이터를 컨텍스트에 저장
                context.add_artifact("result_data_sample", transformed_data.head(5).to_dict())
                
                logger.info("파이프라인 실행 완료")
            except Exception as e:
                logger.error(f"파이프라인 실행 중 오류 발생: {str(e)}")
                logger.debug(traceback.format_exc())
                raise
        
        return self.context.status, self.context
    
    def run_batch(self, batch_size: Optional[int] = None) -> Tuple[ExecutionStatus, PipelineContext]:
        """배치 처리 모드로 파이프라인 실행
        
        Args:
            batch_size: 배치 크기 (기본값: 변수에서 자동 추출)
            
        Returns:
            실행 상태와 컨텍스트 정보
        """
        # 배치 크기가 지정되지 않은 경우 변수에서 추출
        if batch_size is None:
            batch_size = self.pipeline_config.variables.get("batch_size", 10000)
        
        logger.info(f"배치 모드로 파이프라인 시작: {self.pipeline_config.name} (배치 크기: {batch_size})")
        
        # 컨텍스트 매니저를 통한 실행
        with self._pipeline_context() as context:
            try:
                # 컴포넌트 초기화
                self._init_components()
                
                # 배치 처리 모드로 실행
                logger.info("배치 처리 시작")
                total_rows = 0
                batch_count = 0
                
                # 배치 단위로 데이터 처리
                for batch_data in self._extractor.extract_batch(batch_size):
                    batch_count += 1
                    logger.info(f"배치 {batch_count} 처리 중 ({len(batch_data)} 행)")
                    
                    # 변환 및 적재
                    transformed_data = self._transform_data(batch_data)
                    rows_loaded = self._load_data(transformed_data)
                    total_rows += rows_loaded
                    
                    # 배치 처리 진행 상황 기록
                    context.log_event(
                        "batch_complete",
                        f"배치 {batch_count} 처리 완료 ({rows_loaded} 행)",
                        {"batch_number": batch_count, "rows": rows_loaded}
                    )
                
                # 배치 처리 완료
                logger.info(f"배치 처리 완료: 총 {batch_count} 배치, {total_rows} 행")
                context.set_variable("total_batches", batch_count)
                context.set_variable("total_rows", total_rows)
                
            except Exception as e:
                logger.error(f"배치 처리 중 오류 발생: {str(e)}")
                logger.debug(traceback.format_exc())
                raise
        
        return self.context.status, self.context
    
    def validate(self) -> bool:
        """파이프라인 설정 및 컴포넌트 유효성 검사
        
        Returns:
            검증 성공 여부
        """
        logger.info(f"파이프라인 유효성 검사 시작: {self.pipeline_config.name}")
        
        try:
            # 플러그인 검색
            discover_plugins()
            
            # Extractor 검증
            source_config = self.pipeline_config.source
            source_settings = source_config.config if hasattr(source_config, 'config') else source_config.model_dump()
            if 'type' in source_settings:
                del source_settings['type']
            extractor = create_extractor(source_config.type, source_settings)
            
            # Loader 검증
            destination_config = self.pipeline_config.destination
            destination_settings = destination_config.config if hasattr(destination_config, 'config') else destination_config.model_dump()
            if 'type' in destination_settings:
                del destination_settings['type']
            loader = create_loader(destination_config.type, destination_settings)
            
            # 샘플 데이터로 파이프라인 흐름 검증
            logger.info("샘플 데이터로 파이프라인 검증 중...")
            sample_data = extractor.extract_sample(5)
            
            # 변환기가 있는 경우 검증 (구현 필요)
            if self.pipeline_config.transformer:
                # TODO: Transformer 검증 로직 추가
                pass
            
            # 스키마 호환성 검증
            extractor_schema = extractor.get_schema()
            
            logger.info("파이프라인 유효성 검사 완료")
            return True
        except Exception as e:
            logger.error(f"파이프라인 유효성 검사 실패: {str(e)}")
            logger.debug(traceback.format_exc())
            return False 
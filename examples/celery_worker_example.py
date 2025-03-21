#!/usr/bin/env python
"""
Celery 작업자 실행 예제

이 예제는 dteg Celery 작업자를 실행하고 파이프라인 작업을 제출하는 방법을 보여줍니다.
"""
import logging
import sys
import time
from pathlib import Path
import uuid
import argparse
import json

from dteg.orchestration.worker import CeleryTaskManager, run_pipeline
from dteg.core.config import PipelineConfig

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Celery 파이프라인 작업 실행 예제")
    parser.add_argument("--submit", action="store_true", help="파이프라인 작업 제출")
    parser.add_argument("--status", type=str, help="작업 상태 확인 (작업 ID 필요)")
    parser.add_argument("--revoke", type=str, help="작업 취소 (작업 ID 필요)")
    args = parser.parse_args()
    
    # 테스트 설정 파일 경로
    examples_dir = Path(__file__).parent
    test_pipeline_config = examples_dir.parent / "test_pipeline" / "example_pipeline.yaml"
    
    # 테스트 파이프라인 설정 파일이 없으면 생성
    if not test_pipeline_config.exists():
        logger.error(f"테스트 파이프라인 설정 파일을 찾을 수 없습니다: {test_pipeline_config}")
        logger.info("테스트용 설정 파일을 생성합니다.")
        
        # 테스트용 디렉토리 및 설정 파일 생성
        test_dir = examples_dir.parent / "test_pipeline"
        test_dir.mkdir(exist_ok=True)
        
        # 간단한 테스트 파이프라인 설정 작성
        test_config_content = """
pipeline_id: example_pipeline
description: 예제 파이프라인

extractor:
  type: csv
  config:
    file_path: ${examples_dir}/data/sample.csv
    
loader:
  type: csv
  config:
    file_path: ${examples_dir}/output/output.csv
"""
        with open(test_pipeline_config, 'w', encoding='utf-8') as f:
            f.write(test_config_content)
            
        # 테스트 데이터 디렉토리 생성
        data_dir = examples_dir / "data"
        data_dir.mkdir(exist_ok=True)
        
        # 테스트 데이터 파일 생성
        sample_data = """id,name,value
1,item1,100
2,item2,200
3,item3,300
"""
        sample_file = data_dir / "sample.csv"
        with open(sample_file, 'w', encoding='utf-8') as f:
            f.write(sample_data)
            
        # 출력 디렉토리 생성
        output_dir = examples_dir / "output"
        output_dir.mkdir(exist_ok=True)
        
        logger.info(f"테스트 파일 생성 완료: {test_pipeline_config}")
        logger.info(f"샘플 데이터 생성 완료: {sample_file}")
    
    # Celery 작업 관리자 생성
    task_manager = CeleryTaskManager()
    
    if args.submit:
        # 작업 제출
        execution_id = str(uuid.uuid4())
        task_id = task_manager.run_pipeline(
            pipeline_config=test_pipeline_config,
            execution_id=execution_id
        )
        
        logger.info(f"파이프라인 작업이 제출되었습니다.")
        logger.info(f"  작업 ID: {task_id}")
        logger.info(f"  실행 ID: {execution_id}")
        logger.info(f"결과 확인: python {__file__} --status {task_id}")
        
        # 작업 ID를 파일에 저장
        with open(examples_dir / "last_task_id.txt", 'w') as f:
            f.write(task_id)
        
    elif args.status:
        # 작업 상태 확인
        task_id = args.status
        logger.info(f"작업 상태 확인 중: {task_id}")
        
        # 결과 조회 (결과가 준비될 때까지 최대 5초 대기)
        result = task_manager.get_result(task_id, wait=True, timeout=5)
        
        logger.info(f"작업 상태: {result['status']}")
        if result['status'] == 'success':
            logger.info(f"실행 결과: {json.dumps(result.get('result', {}), indent=2)}")
        elif result['status'] == 'failed':
            logger.error(f"실행 실패: {result.get('error', '알 수 없는 오류')}")
        else:
            logger.info(f"작업이 아직 완료되지 않았습니다.")
            
    elif args.revoke:
        # 작업 취소
        task_id = args.revoke
        logger.info(f"작업 취소 중: {task_id}")
        
        if task_manager.revoke_task(task_id, terminate=True):
            logger.info(f"작업이 취소되었습니다: {task_id}")
        else:
            logger.error(f"작업 취소 실패: {task_id}")
            
    else:
        # 기본 안내 출력
        logger.info("사용법:")
        logger.info(f"  작업 제출: python {__file__} --submit")
        logger.info(f"  작업 상태 확인: python {__file__} --status <task_id>")
        logger.info(f"  작업 취소: python {__file__} --revoke <task_id>")
        
        # 저장된 마지막 작업 ID가 있으면 표시
        last_task_file = examples_dir / "last_task_id.txt"
        if last_task_file.exists():
            with open(last_task_file, 'r') as f:
                last_task_id = f.read().strip()
            logger.info(f"마지막 작업 ID: {last_task_id}")
            logger.info(f"  상태 확인: python {__file__} --status {last_task_id}")

if __name__ == "__main__":
    main() 
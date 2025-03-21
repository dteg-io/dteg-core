#!/usr/bin/env python
"""
스케줄러 사용 예제

이 예제는 dteg 스케줄러를 사용하여 파이프라인을 스케줄링하는 방법을 보여줍니다.
"""
import logging
import sys
import time
from pathlib import Path

from dteg.core.config import PipelineConfig
from dteg.orchestration.scheduler import Scheduler, ScheduleConfig, ExecutionRecord

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def execution_callback(record: ExecutionRecord):
    """실행 완료 콜백 함수"""
    status = "성공" if record.status == "SUCCESS" else "실패"
    logger.info(f"파이프라인 {record.pipeline_id} 실행 {status}!")
    
    if record.status != "SUCCESS":
        logger.error(f"오류: {record.error_message}")

def main():
    # 테스트 설정 파일 경로
    examples_dir = Path(__file__).parent
    test_pipeline_config = examples_dir.parent / "test_pipeline" / "example_pipeline.yaml"
    
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
    
    # 스케줄러 생성
    scheduler = Scheduler(on_execution_complete=execution_callback)
    
    # 테스트 파이프라인 스케줄 추가
    # 매 1분마다 실행되는 스케줄
    schedule_config = ScheduleConfig(
        pipeline_config=test_pipeline_config,
        cron_expression="*/1 * * * *",  # 매분 실행
        enabled=True
    )
    schedule_id = scheduler.add_schedule(schedule_config)
    
    # 의존성이 있는 두 번째 스케줄 추가
    schedule_config2 = ScheduleConfig(
        pipeline_config=test_pipeline_config,
        cron_expression="*/2 * * * *",  # 2분마다 실행
        dependencies=[schedule_id],  # 첫 번째 스케줄에 의존
        enabled=True
    )
    scheduler.add_schedule(schedule_config2)
    
    logger.info("스케줄러 시작됨... Ctrl+C로 중지")
    
    # 테스트를 위해 즉시 실행 트리거
    logger.info("첫 번째 실행을 트리거합니다...")
    scheduler.run_once()
    
    # 스케줄러 실행 (60초마다 스케줄 확인)
    try:
        # 실제 운영 환경에서는 scheduler.run_scheduler()를 사용
        # 예제에서는 5분 후 종료
        max_runtime = 300  # 5분
        start_time = time.time()
        
        while time.time() - start_time < max_runtime:
            scheduler.run_once()
            time.sleep(10)  # 10초마다 확인 (예제용)
            
        logger.info("예제 실행 완료 (5분 경과)")
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중지됨")
    
if __name__ == "__main__":
    main() 
#!/usr/bin/env python
"""
오케스트레이션 관리자 사용 예제

이 예제는 dteg 오케스트레이션 관리자를 사용하여 파이프라인을 관리하는 방법을 보여줍니다.
"""
import logging
import sys
import time
from pathlib import Path
import argparse
import json
from datetime import datetime, timedelta

from dteg.orchestration.orchestrator import Orchestrator
from dteg.core.config import PipelineConfig

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def execution_callback(execution_info):
    """파이프라인 실행 완료 콜백 함수"""
    status = "성공" if execution_info["status"] == "SUCCESS" else "실패"
    logger.info(f"파이프라인 {execution_info['pipeline_id']} 실행 {status}!")
    logger.info(f"실행 ID: {execution_info['execution_id']}")
    
    if execution_info["status"] != "SUCCESS":
        logger.error(f"오류: {execution_info.get('error_message', '알 수 없는 오류')}")

def prepare_test_files():
    """테스트 파일 생성"""
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
    
    return test_pipeline_config

def main():
    parser = argparse.ArgumentParser(description="오케스트레이션 관리자 예제")
    parser.add_argument("--add", action="store_true", help="파이프라인 스케줄 추가")
    parser.add_argument("--run", action="store_true", help="파이프라인 즉시 실행")
    parser.add_argument("--status", type=str, help="파이프라인 상태 확인 (실행 ID)")
    parser.add_argument("--list", action="store_true", help="모든 파이프라인 목록 조회")
    parser.add_argument("--schedule", action="store_true", help="스케줄러 시작")
    parser.add_argument("--celery", action="store_true", help="Celery 사용 여부")
    args = parser.parse_args()
    
    # 테스트 파일 준비
    test_pipeline_config = prepare_test_files()
    
    # 오케스트레이션 관리자 생성
    orchestrator = Orchestrator(
        use_celery=args.celery,
        on_execution_complete=execution_callback
    )
    
    if args.add:
        # 파이프라인 스케줄 추가
        schedule_id = orchestrator.add_pipeline(
            pipeline_config=test_pipeline_config,
            cron_expression="*/5 * * * *",  # 5분마다 실행
            enabled=True
        )
        
        logger.info(f"파이프라인 스케줄이 추가되었습니다.")
        logger.info(f"  스케줄 ID: {schedule_id}")
        
        # 두 번째 파이프라인 추가 (의존성 있음)
        schedule_id2 = orchestrator.add_pipeline(
            pipeline_config=test_pipeline_config,
            cron_expression="*/10 * * * *",  # 10분마다 실행
            dependencies=[schedule_id],
            enabled=True
        )
        
        logger.info(f"두 번째 파이프라인 스케줄이 추가되었습니다.")
        logger.info(f"  스케줄 ID: {schedule_id2}")
        logger.info(f"  의존성: {schedule_id}")
        
    elif args.run:
        # 즉시 실행
        pipelines = orchestrator.get_all_pipelines()
        
        if not pipelines:
            # 스케줄이 없으면 추가
            schedule_id = orchestrator.add_pipeline(
                pipeline_config=test_pipeline_config,
                cron_expression="*/5 * * * *",
                enabled=True
            )
            logger.info(f"파이프라인 스케줄이 추가되었습니다: {schedule_id}")
        else:
            schedule_id = pipelines[0]["schedule_id"]
        
        # 파이프라인 실행
        result = orchestrator.run_pipeline(
            pipeline_id=schedule_id, 
            async_execution=args.celery
        )
        
        logger.info(f"파이프라인 실행이 시작되었습니다.")
        
        if args.celery:
            logger.info(f"  작업 ID: {result.get('task_id')}")
            logger.info(f"  실행 ID: {result.get('execution_id')}")
            logger.info(f"Celery 워커가 실행 중인지 확인하세요!")
        else:
            logger.info(f"  실행 ID: {result.get('execution_id')}")
            logger.info(f"  상태: {result.get('status')}")
        
    elif args.status and args.status != "true":
        # 파이프라인 상태 확인
        execution_id = args.status
        status = orchestrator.get_pipeline_status(execution_id=execution_id)
        
        logger.info(f"파이프라인 실행 상태:")
        logger.info(f"  실행 ID: {status.get('execution_id')}")
        logger.info(f"  상태: {status.get('status')}")
        
        if status.get('status') == "failed":
            logger.error(f"  오류: {status.get('error_message', '알 수 없는 오류')}")
        
    elif args.list:
        # 모든 파이프라인 목록 조회
        pipelines = orchestrator.get_all_pipelines()
        
        if not pipelines:
            logger.info("등록된 파이프라인이 없습니다.")
        else:
            logger.info(f"등록된 파이프라인 목록 ({len(pipelines)}개):")
            
            for i, pipeline in enumerate(pipelines, 1):
                next_run = datetime.fromisoformat(pipeline["next_run"])
                now = datetime.now()
                time_diff = next_run - now
                
                if time_diff.total_seconds() < 0:
                    next_run_str = "곧 실행"
                else:
                    hours, remainder = divmod(time_diff.seconds, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    next_run_str = f"{hours}시간 {minutes}분 {seconds}초 후"
                
                logger.info(f"  {i}. 파이프라인 ID: {pipeline['pipeline_id']}")
                logger.info(f"     스케줄 ID: {pipeline['schedule_id']}")
                logger.info(f"     Cron: {pipeline['cron_expression']}")
                logger.info(f"     다음 실행: {next_run_str}")
                logger.info(f"     활성화: {'예' if pipeline['enabled'] else '아니오'}")
                
                if pipeline['dependencies']:
                    logger.info(f"     의존성: {', '.join(pipeline['dependencies'])}")
                
                logger.info("")
        
    elif args.schedule:
        # 스케줄러 시작
        logger.info("스케줄러를 시작합니다... (Ctrl+C로 중지)")
        
        # 스케줄이 없으면 추가
        pipelines = orchestrator.get_all_pipelines()
        if not pipelines:
            schedule_id = orchestrator.add_pipeline(
                pipeline_config=test_pipeline_config,
                cron_expression="*/1 * * * *",  # 1분마다 실행 (테스트용)
                enabled=True
            )
            logger.info(f"테스트 파이프라인 스케줄이 추가되었습니다: {schedule_id}")
        
        # 스케줄러 시작
        orchestrator.start_scheduler(interval=30)  # 30초마다 확인 (테스트용)
        
        try:
            # 예제에서는 5분 동안만 실행
            max_runtime = 300  # 5분
            start_time = time.time()
            
            while time.time() - start_time < max_runtime:
                time.sleep(1)
                
            logger.info("예제 실행 완료 (5분 경과)")
            orchestrator.stop_scheduler()
            
        except KeyboardInterrupt:
            logger.info("사용자에 의해 중지됨")
            orchestrator.stop_scheduler()
    
    else:
        # 기본 안내 출력
        logger.info("사용법:")
        logger.info(f"  스케줄 추가: python {__file__} --add")
        logger.info(f"  즉시 실행: python {__file__} --run [--celery]")
        logger.info(f"  상태 확인: python {__file__} --status <execution_id>")
        logger.info(f"  목록 조회: python {__file__} --list")
        logger.info(f"  스케줄러 시작: python {__file__} --schedule [--celery]")
        logger.info(f"  Celery 사용: --celery 옵션 추가")

if __name__ == "__main__":
    main() 
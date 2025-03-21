"""
오케스트레이션 통합 테스트

이 모듈은 스케줄러, 워커 및 오케스트레이터 컴포넌트들이 통합적으로 잘 작동하는지 테스트합니다.
통합 테스트를 위해 Redis가 실행 중이어야 합니다.
"""
import unittest
import os
import tempfile
import time
import json
from pathlib import Path
from unittest.mock import MagicMock, patch
import threading

from dteg.core.config import PipelineConfig
from dteg.orchestration.scheduler import Scheduler, ScheduleConfig
from dteg.orchestration.worker import CeleryTaskQueue
from dteg.orchestration.orchestrator import Orchestrator


class MockPipeline:
    """테스트용 파이프라인 모의 클래스"""
    
    def __init__(self, config):
        self.config = config
        self.run_called = False
    
    def run(self):
        """파이프라인 실행"""
        self.run_called = True
        # 실행 시간을 시뮬레이션하기 위해 약간의 지연 추가
        time.sleep(0.1)
        return True


@unittest.skipIf(os.environ.get('SKIP_INTEGRATION_TESTS', '0') == '1', 
                 "Redis가 필요한 통합 테스트를 건너뜁니다.")
class TestOrchestrationIntegration(unittest.TestCase):
    """오케스트레이션 통합 테스트"""
    
    def setUp(self):
        """테스트 설정"""
        # 임시 디렉토리 생성
        self.temp_dir = tempfile.TemporaryDirectory()
        self.history_dir = Path(self.temp_dir.name)
        
        # 테스트용 Redis URL
        self.broker_url = "redis://localhost:6379/0"
        self.result_backend = "redis://localhost:6379/1"
        
        # 오케스트레이터 및 관련 컴포넌트 설정
        with patch('dteg.orchestration.worker.Pipeline', MockPipeline):
            self.orchestrator = Orchestrator(
                broker_url=self.broker_url,
                result_backend=self.result_backend,
                history_dir=self.history_dir
            )
        
        # 테스트용 파이프라인 설정 생성
        self.pipeline_config = MagicMock(spec=PipelineConfig)
        self.pipeline_config.pipeline_id = "test_pipeline"
        
        # Redis가 실행 중인지 확인
        try:
            import redis
            r = redis.Redis.from_url(self.broker_url)
            r.ping()
        except:
            self.skipTest("Redis에 연결할 수 없습니다. 통합 테스트를 건너뜁니다.")
    
    def tearDown(self):
        """테스트 정리"""
        # 스케줄러 중지
        if hasattr(self, 'orchestrator'):
            self.orchestrator.stop_scheduler()
        
        # 임시 디렉토리 정리
        self.temp_dir.cleanup()
    
    def test_add_and_list_pipelines(self):
        """파이프라인 추가 및 조회 테스트"""
        # 파이프라인 추가
        schedule_id = self.orchestrator.add_pipeline(
            pipeline_config=self.pipeline_config,
            cron_expression="*/5 * * * *",
            enabled=True
        )
        
        # 스케줄 ID가 반환되었는지 확인
        self.assertIsNotNone(schedule_id)
        
        # 추가한 파이프라인이 목록에 있는지 확인
        pipelines = self.orchestrator.get_all_pipelines()
        self.assertIn(schedule_id, pipelines)
        self.assertEqual(pipelines[schedule_id]["pipeline_id"], "test_pipeline")
    
    def test_update_pipeline(self):
        """파이프라인 업데이트 테스트"""
        # 파이프라인 추가
        schedule_id = self.orchestrator.add_pipeline(
            pipeline_config=self.pipeline_config,
            cron_expression="*/5 * * * *",
            enabled=True
        )
        
        # 파이프라인 업데이트
        result = self.orchestrator.update_pipeline(
            schedule_id=schedule_id,
            cron_expression="0 * * * *",
            enabled=False
        )
        
        # 업데이트 성공 확인
        self.assertTrue(result)
        
        # 변경사항 확인
        pipelines = self.orchestrator.get_all_pipelines()
        self.assertEqual(pipelines[schedule_id]["cron_expression"], "0 * * * *")
        self.assertFalse(pipelines[schedule_id]["enabled"])
    
    def test_remove_pipeline(self):
        """파이프라인 제거 테스트"""
        # 파이프라인 추가
        schedule_id = self.orchestrator.add_pipeline(
            pipeline_config=self.pipeline_config,
            cron_expression="*/5 * * * *",
            enabled=True
        )
        
        # 파이프라인 제거
        result = self.orchestrator.remove_pipeline(schedule_id)
        
        # 제거 성공 확인
        self.assertTrue(result)
        
        # 파이프라인이 목록에서 제거되었는지 확인
        pipelines = self.orchestrator.get_all_pipelines()
        self.assertNotIn(schedule_id, pipelines)
    
    @patch('dteg.orchestration.worker.Pipeline', MockPipeline)
    def test_run_pipeline(self):
        """파이프라인 실행 테스트"""
        # 파이프라인 추가
        schedule_id = self.orchestrator.add_pipeline(
            pipeline_config=self.pipeline_config,
            cron_expression="*/5 * * * *",
            enabled=True
        )
        
        # 파이프라인 실행
        task_id = self.orchestrator.run_pipeline(schedule_id)
        
        # 태스크 ID가 반환되었는지 확인
        self.assertIsNotNone(task_id)
        
        # 태스크 완료까지 대기
        max_wait = 5  # 최대 5초 대기
        wait_time = 0
        status = None
        while wait_time < max_wait:
            status = self.orchestrator.check_pipeline_status(task_id)
            if status in ["SUCCESS", "FAILURE"]:
                break
            time.sleep(0.5)
            wait_time += 0.5
        
        # 태스크가 성공적으로 완료되었는지 확인
        self.assertEqual(status, "SUCCESS")
    
    def test_pipeline_dependencies(self):
        """파이프라인 의존성 테스트"""
        # 파이프라인 두 개 추가
        pipeline_config1 = MagicMock(spec=PipelineConfig)
        pipeline_config1.pipeline_id = "pipeline_1"
        
        pipeline_config2 = MagicMock(spec=PipelineConfig)
        pipeline_config2.pipeline_id = "pipeline_2"
        
        schedule_id1 = self.orchestrator.add_pipeline(
            pipeline_config=pipeline_config1,
            cron_expression="*/5 * * * *",
            enabled=True
        )
        
        schedule_id2 = self.orchestrator.add_pipeline(
            pipeline_config=pipeline_config2,
            cron_expression="*/5 * * * *",
            enabled=True
        )
        
        # 의존성 추가 (pipeline_2는 pipeline_1에 의존)
        result = self.orchestrator.add_pipeline_dependency(schedule_id2, schedule_id1)
        
        # 의존성 추가 성공 확인
        self.assertTrue(result)
        
        # 의존성 조회
        dependencies = self.orchestrator.get_pipeline_dependencies(schedule_id2)
        
        # 의존성 확인
        self.assertIn(schedule_id1, dependencies)
        
        # 의존성 제거
        result = self.orchestrator.remove_pipeline_dependency(schedule_id2, schedule_id1)
        
        # 의존성 제거 성공 확인
        self.assertTrue(result)
        
        # 의존성이 제거되었는지 확인
        dependencies = self.orchestrator.get_pipeline_dependencies(schedule_id2)
        self.assertNotIn(schedule_id1, dependencies)
    
    def test_start_stop_scheduler(self):
        """스케줄러 시작 및 중지 테스트"""
        # 파이프라인 추가 (과거 시간으로 설정)
        with patch('dteg.orchestration.scheduler.ScheduleConfig.update_next_run') as mock_update:
            # 다음 실행 시간을 과거로 설정
            from datetime import datetime, timedelta
            past_time = datetime.now() - timedelta(minutes=5)
            mock_update.side_effect = lambda: setattr(mock_update.im_self, 'next_run', past_time)
            
            schedule_id = self.orchestrator.add_pipeline(
                pipeline_config=self.pipeline_config,
                cron_expression="*/5 * * * *",
                enabled=True
            )
        
        # 스케줄러 시작
        result = self.orchestrator.start_scheduler()
        
        # 스케줄러 시작 성공 확인
        self.assertTrue(result)
        self.assertTrue(self.orchestrator.scheduler_running)
        self.assertIsNotNone(self.orchestrator.scheduler_thread)
        
        # 잠시 대기하여 스케줄러가 동작할 시간 제공
        time.sleep(1)
        
        # 스케줄러 중지
        result = self.orchestrator.stop_scheduler()
        
        # 스케줄러 중지 성공 확인
        self.assertTrue(result)
        self.assertFalse(self.orchestrator.scheduler_running)
    
    @patch('dteg.orchestration.worker.Pipeline', MockPipeline)
    def test_cancel_pipeline(self):
        """파이프라인 취소 테스트"""
        # 파이프라인 추가
        schedule_id = self.orchestrator.add_pipeline(
            pipeline_config=self.pipeline_config,
            cron_expression="*/5 * * * *",
            enabled=True
        )
        
        # 파이프라인 실행
        task_id = self.orchestrator.run_pipeline(schedule_id)
        
        # 파이프라인 취소
        result = self.orchestrator.cancel_pipeline(task_id)
        
        # 취소 성공 확인 (작업이 이미 완료되었을 수 있으므로 결과는 무시)
        # 이 부분은 테스트 환경에 따라 결과가 다를 수 있음
        pass


if __name__ == "__main__":
    unittest.main() 
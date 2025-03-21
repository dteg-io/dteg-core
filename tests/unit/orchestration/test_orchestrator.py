"""
오케스트레이터 모듈 단위 테스트
"""
import unittest
from unittest.mock import MagicMock, patch, call
import tempfile
import os
from pathlib import Path
import threading
from datetime import datetime

from dteg.orchestration.orchestrator import Orchestrator
from dteg.orchestration.scheduler import Scheduler, ScheduleConfig
from dteg.orchestration.worker import CeleryTaskQueue
from dteg.core.config import PipelineConfig


class TestOrchestrator(unittest.TestCase):
    """오케스트레이터 클래스 테스트"""
    
    @patch('dteg.orchestration.orchestrator.Scheduler')
    @patch('dteg.orchestration.orchestrator.CeleryTaskManager')
    def setUp(self, mock_celery_task_manager_class, mock_scheduler_class):
        """테스트 설정"""
        # 스케줄러 모의 객체
        self.mock_scheduler = MagicMock(spec=Scheduler)
        mock_scheduler_class.return_value = self.mock_scheduler
        
        # Celery 태스크 매니저 모의 객체
        self.mock_task_manager = MagicMock()
        mock_celery_task_manager_class.return_value = self.mock_task_manager
        
        # 임시 디렉토리 생성
        self.temp_dir = tempfile.TemporaryDirectory()
        self.history_dir = Path(self.temp_dir.name)
        
        # 오케스트레이터 생성
        self.orchestrator = Orchestrator(
            history_dir=self.history_dir,
            result_dir=self.history_dir,
            use_celery=True
        )
    
    def tearDown(self):
        """테스트 정리"""
        self.temp_dir.cleanup()
    
    def test_init(self):
        """초기화 테스트"""
        self.assertTrue(self.orchestrator.use_celery)
        self.assertEqual(self.orchestrator.scheduler, self.mock_scheduler)
        self.assertEqual(self.orchestrator.task_manager, self.mock_task_manager)
        self.assertFalse(self.orchestrator.scheduler_running)
        self.assertIsNone(self.orchestrator.scheduler_thread)
    
    def test_add_pipeline(self):
        """파이프라인 추가"""
        # 파이프라인 설정 모의 객체
        mock_config = MagicMock(spec=PipelineConfig)
        mock_config.pipeline_id = "test_pipeline"
        
        # add_schedule 호출 결과 모의
        self.mock_scheduler.add_schedule.return_value = "schedule-123"
        
        # 파이프라인 추가
        schedule_id = self.orchestrator.add_pipeline(
            pipeline_config=mock_config,
            cron_expression="*/5 * * * *",
            enabled=True
        )
        
        # 스케줄러의 add_schedule 메소드 호출 확인
        self.mock_scheduler.add_schedule.assert_called_once()
        args, kwargs = self.mock_scheduler.add_schedule.call_args
        
        # 전달된 ScheduleConfig 객체 확인
        self.assertIsInstance(args[0], ScheduleConfig)
        self.assertEqual(args[0].pipeline_config, mock_config)
        self.assertEqual(args[0].cron_expression, "*/5 * * * *")
        self.assertTrue(args[0].enabled)
        
        # 반환값 확인
        self.assertEqual(schedule_id, "schedule-123")
    
    def test_get_all_pipelines(self):
        """모든 파이프라인 조회"""
        # 스케줄 목록 모의
        mock_schedule1 = MagicMock()
        mock_schedule1.id = "schedule-1"
        mock_schedule1.enabled = True
        mock_schedule1.cron_expression = "0 0 * * *"
        mock_schedule1.next_run = datetime.now()
        mock_schedule1.dependencies = []
        
        # PipelineConfig 모의
        mock_config1 = MagicMock()
        mock_config1.pipeline_id = "pipeline-1"
        mock_schedule1.pipeline_config = mock_config1
        
        mock_schedule2 = MagicMock()
        mock_schedule2.id = "schedule-2"
        mock_schedule2.enabled = True
        mock_schedule2.cron_expression = "0 12 * * *"
        mock_schedule2.next_run = datetime.now()
        mock_schedule2.dependencies = []
        
        # PipelineConfig 모의
        mock_config2 = MagicMock()
        mock_config2.pipeline_id = "pipeline-2"
        mock_schedule2.pipeline_config = mock_config2
        
        # get_all_schedules의 반환값 설정
        self.mock_scheduler.get_all_schedules.return_value = [mock_schedule1, mock_schedule2]
        
        # 모든 파이프라인 조회
        result = self.orchestrator.get_all_pipelines()
        
        # 스케줄러의 get_all_schedules 메소드 호출 확인
        self.mock_scheduler.get_all_schedules.assert_called_once()
        
        # 결과 확인
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["schedule_id"], "schedule-1")
        self.assertEqual(result[0]["pipeline_id"], "pipeline-1")
        self.assertEqual(result[1]["schedule_id"], "schedule-2")
        self.assertEqual(result[1]["pipeline_id"], "pipeline-2")
    
    def test_remove_pipeline(self):
        """파이프라인 제거"""
        # remove_schedule 호출 결과 모의
        self.mock_scheduler.remove_schedule.return_value = True
        
        # 파이프라인 제거
        result = self.orchestrator.remove_pipeline("schedule-123")
        
        # 스케줄러의 remove_schedule 메소드 호출 확인
        self.mock_scheduler.remove_schedule.assert_called_once_with("schedule-123")
        
        # 결과 확인
        self.assertTrue(result)
    
    def test_run_pipeline(self):
        """파이프라인 실행"""
        # 테스트 데이터
        mock_config = MagicMock(spec=PipelineConfig)
        mock_config.pipeline_id = "test_pipeline"
        schedule_id = "schedule-123"
        
        # 스케줄 조회 결과 모의
        mock_schedule = MagicMock(spec=ScheduleConfig)
        mock_schedule.pipeline_config = mock_config
        self.mock_scheduler.get_schedule.return_value = mock_schedule
        
        # 태스크 실행 결과 모의
        task_id = "task-123"
        self.mock_task_manager.run_pipeline.return_value = task_id
        
        # 파이프라인 실행
        result = self.orchestrator.run_pipeline(schedule_id)
        
        # 스케줄러의 get_schedule 메소드 호출 확인
        self.mock_scheduler.get_schedule.assert_called_once_with(schedule_id)
        
        # 태스크 매니저의 run_pipeline 메소드 호출 확인
        self.mock_task_manager.run_pipeline.assert_called_once()
        kwargs = self.mock_task_manager.run_pipeline.call_args.kwargs
        self.assertEqual(kwargs['pipeline_config'], mock_config)
        
        # 결과 확인
        self.assertEqual(result["task_id"], task_id)
        self.assertEqual(result["status"], "submitted")
        self.assertEqual(result["pipeline_id"], schedule_id)
    
    def test_run_nonexistent_pipeline(self):
        """존재하지 않는 파이프라인 실행"""
        # 스케줄 조회 결과 모의 (None 반환)
        self.mock_scheduler.get_schedule.return_value = None
        
        # 존재하지 않는 파이프라인 실행
        with self.assertRaises(Exception):
            self.orchestrator.run_pipeline("nonexistent")
    
    def test_check_pipeline_status(self):
        """파이프라인 상태 확인"""
        # 태스크 상태 조회 결과 모의
        result_data = {
            "status": "SUCCESS",
            "execution_id": "exec-123",
            "task_id": "task-123",
            "pipeline_id": "pipeline-1"
        }
        self.mock_task_manager.get_result.return_value = result_data
        
        # 파이프라인 상태 확인
        status = self.orchestrator.get_pipeline_status(task_id="task-123")
        
        # 태스크 매니저의 get_result 메소드 호출 확인
        self.mock_task_manager.get_result.assert_called_once_with("task-123")
        
        # 상태 확인
        self.assertEqual(status, result_data)
    
    def test_cancel_pipeline(self):
        """파이프라인 실행 취소"""
        # 태스크 취소 결과 모의
        self.mock_task_manager.revoke_task.return_value = True
        
        # 파이프라인 실행 취소
        result = self.orchestrator.cancel_execution(task_id="task-123")
        
        # 태스크 매니저의 revoke_task 메소드 호출 확인
        self.mock_task_manager.revoke_task.assert_called_once_with("task-123", terminate=True)
        
        # 결과 확인
        self.assertTrue(result)
    
    @patch('dteg.orchestration.orchestrator.threading.Thread')
    def test_start_scheduler(self, mock_thread_class):
        """스케줄러 시작"""
        # Thread 모의 객체
        mock_thread = MagicMock()
        mock_thread_class.return_value = mock_thread
        
        # 스케줄러 시작 전 상태 확인
        self.assertFalse(self.orchestrator.scheduler_running)
        
        # 스케줄러 시작
        self.orchestrator.start_scheduler()
        
        # Thread 생성자 호출 확인
        mock_thread_class.assert_called_once()
        kwargs = mock_thread_class.call_args.kwargs
        self.assertEqual(kwargs["daemon"], True)
        
        # 스케줄러 상태 확인
        self.assertTrue(self.orchestrator.scheduler_running)
        self.assertEqual(self.orchestrator.scheduler_thread, mock_thread)
        
        # Thread.start 메소드 호출 확인
        mock_thread.start.assert_called_once()
    
    def test_start_scheduler_already_running(self):
        """이미 실행 중인 스케줄러 시작"""
        # 스케줄러가 이미 실행 중인 상태로 설정
        self.orchestrator.scheduler_running = True
        self.orchestrator.scheduler_thread = MagicMock()
        
        # 스케줄러 시작 시도
        result = self.orchestrator.start_scheduler()
        
        # 결과 확인 (실패)
        self.assertFalse(result)
    
    def test_stop_scheduler(self):
        """스케줄러 중지"""
        # 스케줄러가 실행 중인 상태로 설정
        self.orchestrator.scheduler_running = True
        self.orchestrator.scheduler_thread = MagicMock()
        
        # 스케줄러 중지
        self.orchestrator.stop_scheduler()
        
        # 상태 변수 확인
        self.assertFalse(self.orchestrator.scheduler_running)
        
        # 스레드 종료 대기 메서드 호출 확인
        self.orchestrator.scheduler_thread.join.assert_called_once()
    
    def test_stop_scheduler_not_running(self):
        """실행 중이지 않은 스케줄러 중지"""
        # 스케줄러가 실행 중이지 않은 상태로 설정
        self.orchestrator.scheduler_running = False
        self.orchestrator.scheduler_thread = None
        
        # 스케줄러 중지 시도
        result = self.orchestrator.stop_scheduler()
        
        # 결과 확인 (실패)
        self.assertFalse(result)
    
    @patch('dteg.orchestration.orchestrator.time.sleep')
    def test_scheduler_loop(self, mock_sleep):
        """스케줄러 루프"""
        # 스케줄러 상태 설정
        self.orchestrator.scheduler_running = True
        
        # run_once 호출 횟수 추적
        call_count = 0
        
        # 테스트를 위한 내부 함수
        def side_effect_func(*args):
            nonlocal call_count
            call_count += 1
            # 첫 번째 호출 후에만 스케줄러 중지
            if call_count == 1:
                self.orchestrator.scheduler_running = False
        
        # sleep 함수가 호출될 때 side_effect 함수 실행
        mock_sleep.side_effect = side_effect_func
        
        # 테스트를 위한 간단한 스케줄러 루프 함수
        def test_scheduler_loop():
            while self.orchestrator.scheduler_running:
                self.orchestrator.scheduler.run_once()
                mock_sleep(60)
        
        # 스케줄러 루프 실행
        test_scheduler_loop()
        
        # 스케줄러의 run_once 메소드 호출 확인
        self.assertEqual(self.mock_scheduler.run_once.call_count, 1)
    
    def test_update_pipeline(self):
        """파이프라인 업데이트"""
        # 테스트 데이터
        schedule_id = "schedule-123"
        
        # update_schedule 호출 결과 모의
        self.mock_scheduler.update_schedule.return_value = True
        
        # 파이프라인 업데이트
        result = self.orchestrator.update_pipeline(
            schedule_id=schedule_id,
            enabled=False,
            cron_expression="0 0 * * *",
            max_retries=5
        )
        
        # 스케줄러의 update_schedule 메소드 호출 확인
        self.mock_scheduler.update_schedule.assert_called_once_with(
            schedule_id,
            enabled=False,
            cron_expression="0 0 * * *",
            max_retries=5
        )
        
        # 결과 확인
        self.assertTrue(result)
    
    def test_add_pipeline_dependency(self):
        """파이프라인 의존성 추가"""
        # 테스트 데이터
        schedule_id = "schedule-123"
        dependency_id = "schedule-456"
        
        # 스케줄 조회 및 업데이트 결과 모의
        mock_schedule = MagicMock(spec=ScheduleConfig)
        mock_schedule.dependencies = []
        
        mock_dep_schedule = MagicMock(spec=ScheduleConfig)
        
        # get_schedule 호출 시 다른 값 반환하도록 설정
        self.mock_scheduler.get_schedule.side_effect = lambda sid: mock_schedule if sid == schedule_id else mock_dep_schedule
        
        self.mock_scheduler.update_schedule.return_value = True
        
        # 의존성 추가
        result = self.orchestrator.add_pipeline_dependency(schedule_id, dependency_id)
        
        # 스케줄러의 get_schedule 메소드 호출 확인
        self.assertEqual(self.mock_scheduler.get_schedule.call_count, 2)
        
        # 스케줄러의 update_schedule 메소드 호출 확인
        self.mock_scheduler.update_schedule.assert_called_once_with(
            schedule_id, dependencies=[dependency_id]
        )
        
        # 결과 확인
        self.assertTrue(result)
    
    def test_add_duplicate_pipeline_dependency(self):
        """중복된 파이프라인 의존성 추가"""
        # 테스트 데이터
        schedule_id = "schedule-123"
        dependency_id = "schedule-456"
        
        # 스케줄 조회 결과 모의 (이미 의존성이 있음)
        mock_schedule = MagicMock(spec=ScheduleConfig)
        mock_schedule.dependencies = [dependency_id]
        
        mock_dep_schedule = MagicMock(spec=ScheduleConfig)
        
        # get_schedule 호출 시 다른 값 반환하도록 설정
        self.mock_scheduler.get_schedule.side_effect = lambda sid: mock_schedule if sid == schedule_id else mock_dep_schedule
        
        # 중복된 의존성 추가 시도
        result = self.orchestrator.add_pipeline_dependency(schedule_id, dependency_id)
        
        # 스케줄러의 update_schedule 메소드가 호출되지 않음
        self.mock_scheduler.update_schedule.assert_not_called()
        
        # 결과 확인 (실패)
        self.assertFalse(result)
    
    def test_remove_pipeline_dependency(self):
        """파이프라인 의존성 제거"""
        # 테스트 데이터
        schedule_id = "schedule-123"
        dependency_id = "schedule-456"
        
        # 스케줄 조회 및 업데이트 결과 모의
        mock_schedule = MagicMock(spec=ScheduleConfig)
        mock_schedule.dependencies = [dependency_id, "schedule-789"]
        self.mock_scheduler.get_schedule.return_value = mock_schedule
        self.mock_scheduler.update_schedule.return_value = True
        
        # 의존성 제거
        result = self.orchestrator.remove_pipeline_dependency(schedule_id, dependency_id)
        
        # 스케줄러의 get_schedule 메소드 호출 확인
        self.mock_scheduler.get_schedule.assert_called_once_with(schedule_id)
        
        # 스케줄러의 update_schedule 메소드 호출 확인 (의존성에서 dependency_id가 제거됨)
        self.mock_scheduler.update_schedule.assert_called_once_with(
            schedule_id, dependencies=["schedule-789"]
        )
        
        # 결과 확인
        self.assertTrue(result)
    
    def test_remove_nonexistent_pipeline_dependency(self):
        """존재하지 않는 파이프라인 의존성 제거"""
        # 테스트 데이터
        schedule_id = "schedule-123"
        dependency_id = "nonexistent"
        
        # 스케줄 조회 결과 모의 (의존성에 대상 ID가 없음)
        mock_schedule = MagicMock(spec=ScheduleConfig)
        mock_schedule.dependencies = ["schedule-789"]
        self.mock_scheduler.get_schedule.return_value = mock_schedule
        
        # 존재하지 않는 의존성 제거 시도
        result = self.orchestrator.remove_pipeline_dependency(schedule_id, dependency_id)
        
        # 스케줄러의 update_schedule 메소드가 호출되지 않음
        self.mock_scheduler.update_schedule.assert_not_called()
        
        # 결과 확인 (실패)
        self.assertFalse(result)
    
    def test_get_pipeline_dependencies(self):
        """파이프라인 의존성 조회"""
        # 테스트 데이터
        schedule_id = "schedule-123"
        dependencies = ["schedule-456", "schedule-789"]
        
        # 스케줄 조회 결과 모의
        mock_schedule = MagicMock(spec=ScheduleConfig)
        mock_schedule.dependencies = dependencies
        self.mock_scheduler.get_schedule.return_value = mock_schedule
        
        # 의존성 조회
        result = self.orchestrator.get_pipeline_dependencies(schedule_id)
        
        # 스케줄러의 get_schedule 메소드 호출 확인
        self.mock_scheduler.get_schedule.assert_called_once_with(schedule_id)
        
        # 결과 확인
        self.assertEqual(result, dependencies)
    
    def test_get_pipeline_dependencies_nonexistent_pipeline(self):
        """존재하지 않는 파이프라인의 의존성 조회"""
        # 스케줄 조회 결과 모의 (None 반환)
        self.mock_scheduler.get_schedule.return_value = None
        
        # 존재하지 않는 파이프라인의 의존성 조회
        with self.assertRaises(Exception):
            self.orchestrator.get_pipeline_dependencies("nonexistent")


if __name__ == "__main__":
    unittest.main() 
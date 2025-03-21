"""
스케줄러 모듈 단위 테스트
"""
import unittest
from unittest.mock import MagicMock, patch, Mock
import tempfile
import os
from pathlib import Path
from datetime import datetime, timedelta
import time
from freezegun import freeze_time
import shutil

from dteg.orchestration.scheduler import Scheduler, ScheduleConfig, ExecutionRecord
from dteg.core.config import PipelineConfig


class TestScheduleConfig(unittest.TestCase):
    """스케줄 설정 클래스 테스트"""
    
    def test_init_with_valid_cron(self):
        """유효한 Cron 표현식으로 초기화"""
        mock_config = MagicMock(spec=PipelineConfig)
        mock_config.pipeline_id = "test_pipeline"
        
        schedule = ScheduleConfig(
            pipeline_config=mock_config,
            cron_expression="*/5 * * * *"  # 5분마다 실행
        )
        
        self.assertEqual(schedule.pipeline_config, mock_config)
        self.assertEqual(schedule.cron_expression, "*/5 * * * *")
        self.assertTrue(schedule.enabled)
        self.assertEqual(schedule.max_retries, 3)
        self.assertEqual(schedule.retry_delay, 300)
        self.assertIsNotNone(schedule.next_run)
        
    def test_init_with_invalid_cron(self):
        """유효하지 않은 Cron 표현식으로 초기화 시 예외 발생"""
        mock_config = MagicMock(spec=PipelineConfig)
        
        with self.assertRaises(ValueError):
            ScheduleConfig(
                pipeline_config=mock_config,
                cron_expression="invalid cron"
            )
    
    def test_update_next_run(self):
        """다음 실행 시간 업데이트"""
        mock_config = MagicMock(spec=PipelineConfig)
        mock_config.pipeline_id = "test_pipeline"
        
        # 2023년 1월 1일 오전 10시로 시간 고정
        with freeze_time("2023-01-01 10:00:00"):
            # 매시간 실행되는 스케줄 생성
            schedule = ScheduleConfig(
                pipeline_config=mock_config,
                cron_expression="0 * * * *"  # 매시간 정각
            )
            
            # 최초 생성 시 다음 실행 시간은 2023-01-01 11:00:00
            initial_next_run = schedule.next_run
            expected_initial = datetime(2023, 1, 1, 11, 0, 0)
            self.assertEqual(initial_next_run, expected_initial)
            
            # 시간을 11시 30분으로 변경
            with freeze_time("2023-01-01 11:30:00"):
                schedule.update_next_run()
                
                # 다음 실행 시간은 2023-01-01 12:00:00
                updated_next_run = schedule.next_run
                expected_updated = datetime(2023, 1, 1, 12, 0, 0)
                self.assertEqual(updated_next_run, expected_updated)


class TestExecutionRecord(unittest.TestCase):
    """실행 기록 클래스 테스트"""
    
    def test_init(self):
        """실행 기록 초기화"""
        record = ExecutionRecord(
            schedule_id="schedule-123",
            pipeline_id="pipeline-123"
        )
        
        self.assertEqual(record.schedule_id, "schedule-123")
        self.assertEqual(record.pipeline_id, "pipeline-123")
        self.assertEqual(record.status, "RUNNING")
        self.assertEqual(record.retry_count, 0)
        self.assertIsNone(record.error_message)
        self.assertIsNotNone(record.start_time)
        self.assertIsNone(record.end_time)
    
    def test_complete_success(self):
        """성공적인 실행 완료 처리"""
        record = ExecutionRecord("schedule-123", "pipeline-123")
        record.complete(success=True)
        
        self.assertEqual(record.status, "SUCCESS")
        self.assertIsNone(record.error_message)
        self.assertIsNotNone(record.end_time)
    
    def test_complete_failure(self):
        """실패한 실행 완료 처리"""
        record = ExecutionRecord("schedule-123", "pipeline-123")
        record.complete(success=False, error_message="Error message")
        
        self.assertEqual(record.status, "FAILED")
        self.assertEqual(record.error_message, "Error message")
        self.assertIsNotNone(record.end_time)
    
    def test_retry(self):
        """재시도 처리"""
        record = ExecutionRecord("schedule-123", "pipeline-123")
        record.retry("Retry error")
        
        self.assertEqual(record.status, "RETRYING")
        self.assertEqual(record.retry_count, 1)
        self.assertEqual(record.error_message, "Retry error")
    
    def test_to_dict(self):
        """사전 형태로 변환"""
        record = ExecutionRecord("schedule-123", "pipeline-123")
        record_dict = record.to_dict()
        
        self.assertEqual(record_dict["schedule_id"], "schedule-123")
        self.assertEqual(record_dict["pipeline_id"], "pipeline-123")
        self.assertEqual(record_dict["status"], "RUNNING")
        self.assertEqual(record_dict["retry_count"], 0)
        self.assertIsNone(record_dict["error_message"])
        self.assertIsNotNone(record_dict["start_time"])
        self.assertIsNone(record_dict["end_time"])


class TestScheduler(unittest.TestCase):
    """스케줄러 클래스 테스트"""
    
    def setUp(self):
        """테스트 설정"""
        self.mock_config = Mock(spec=PipelineConfig)
        self.mock_config.pipeline_id = "test-pipeline"
        
        # 콜백 함수 모의 객체 생성
        self.mock_callback = Mock()
        
        # 임시 디렉토리 생성
        self.temp_dir = tempfile.mkdtemp()
        history_dir = Path(self.temp_dir) / "history"
        schedule_dir = Path(self.temp_dir) / "schedules"
        history_dir.mkdir(parents=True, exist_ok=True)
        schedule_dir.mkdir(parents=True, exist_ok=True)
        
        # _load_schedules 메서드 패치
        self.load_schedules_patcher = patch('dteg.orchestration.scheduler.Scheduler._load_schedules')
        self.mock_load_schedules = self.load_schedules_patcher.start()
        
        # 스케줄러 생성
        self.scheduler = Scheduler(history_dir=history_dir, schedule_dir=schedule_dir)
        
        # 콜백 설정
        self.scheduler.on_execution_complete = self.mock_callback
        
        # 기본 스케줄 생성
        self.schedule = ScheduleConfig(
            pipeline_config=self.mock_config,
            cron_expression="0 8 * * *"  # 매일 오전 8시
        )
    
    def tearDown(self):
        """테스트 정리"""
        # 패치 중지
        self.load_schedules_patcher.stop()
        
        # 임시 디렉토리 삭제
        shutil.rmtree(self.temp_dir)
    
    def test_add_schedule(self):
        """스케줄 추가"""
        schedule_id = self.scheduler.add_schedule(self.schedule)
        
        self.assertEqual(len(self.scheduler.schedules), 1)
        self.assertEqual(self.scheduler.schedules[schedule_id], self.schedule)
    
    def test_remove_schedule(self):
        """스케줄 제거"""
        # 스케줄 추가
        schedule_id = self.scheduler.add_schedule(self.schedule)
        
        # 스케줄 제거
        result = self.scheduler.remove_schedule(schedule_id)
        
        self.assertTrue(result)
        self.assertEqual(len(self.scheduler.schedules), 0)
    
    def test_remove_nonexistent_schedule(self):
        """존재하지 않는 스케줄 제거"""
        result = self.scheduler.remove_schedule("nonexistent")
        
        self.assertFalse(result)
    
    def test_get_schedule(self):
        """스케줄 조회"""
        # 스케줄 추가
        schedule_id = self.scheduler.add_schedule(self.schedule)
        
        # 스케줄 조회
        schedule = self.scheduler.get_schedule(schedule_id)
        
        self.assertEqual(schedule, self.schedule)
    
    def test_get_nonexistent_schedule(self):
        """존재하지 않는 스케줄 조회"""
        schedule = self.scheduler.get_schedule("nonexistent")
        
        self.assertIsNone(schedule)
    
    def test_get_all_schedules(self):
        """모든 스케줄 조회"""
        # 스케줄 추가
        self.scheduler.add_schedule(self.schedule)
        
        # 스케줄 2개 더 추가
        schedule2 = ScheduleConfig(
            pipeline_config=self.mock_config,
            cron_expression="0 0 * * *",  # 매일 자정
            enabled=True
        )
        schedule3 = ScheduleConfig(
            pipeline_config=self.mock_config,
            cron_expression="0 12 * * *",  # 매일 정오
            enabled=False
        )
        self.scheduler.add_schedule(schedule2)
        self.scheduler.add_schedule(schedule3)
        
        # 모든 스케줄 조회
        schedules = self.scheduler.get_all_schedules()
        
        self.assertEqual(len(schedules), 3)
    
    def test_update_schedule(self):
        """스케줄 업데이트"""
        # 스케줄 추가
        schedule_id = self.scheduler.add_schedule(self.schedule)
        
        # 스케줄 업데이트
        result = self.scheduler.update_schedule(
            schedule_id,
            enabled=False,
            cron_expression="0 0 * * *",
            max_retries=5
        )
        
        self.assertTrue(result)
        updated_schedule = self.scheduler.get_schedule(schedule_id)
        self.assertFalse(updated_schedule.enabled)
        self.assertEqual(updated_schedule.cron_expression, "0 0 * * *")
        self.assertEqual(updated_schedule.max_retries, 5)
    
    def test_update_nonexistent_schedule(self):
        """존재하지 않는 스케줄 업데이트"""
        result = self.scheduler.update_schedule(
            "nonexistent",
            enabled=False
        )
        
        self.assertFalse(result)
    
    @patch('dteg.orchestration.scheduler.Scheduler._run_pipeline')
    def test_run_once_with_pending_schedule(self, mock_run_pipeline):
        """실행 대기 중인 스케줄 실행"""
        # 과거 시간으로 다음 실행 시간 설정
        self.schedule.next_run = datetime.now() - timedelta(minutes=1)
        
        # 스케줄 추가
        self.scheduler.add_schedule(self.schedule)
        
        # 스케줄러 실행
        self.scheduler.run_once()
        
        # 파이프라인 실행 함수 호출 확인
        mock_run_pipeline.assert_called_once_with(self.schedule)
    
    @patch('dteg.orchestration.scheduler.Scheduler._run_pipeline')
    def test_run_once_with_disabled_schedule(self, mock_run_pipeline):
        """비활성화된 스케줄은 실행되지 않음"""
        # 과거 시간으로 다음 실행 시간 설정하고 비활성화
        self.schedule.next_run = datetime.now() - timedelta(minutes=1)
        self.schedule.enabled = False
        
        # 스케줄 추가
        self.scheduler.add_schedule(self.schedule)
        
        # 스케줄러 실행
        self.scheduler.run_once()
        
        # 파이프라인 실행 함수가 호출되지 않음
        mock_run_pipeline.assert_not_called()
    
    @patch('dteg.orchestration.scheduler.Pipeline')
    def test_run_pipeline(self, mock_pipeline_class):
        """파이프라인 실행"""
        # Pipeline 클래스의 인스턴스 모의 객체
        mock_pipeline = MagicMock()
        mock_pipeline_class.return_value = mock_pipeline
        
        # 스케줄 추가
        self.scheduler.add_schedule(self.schedule)
        
        # 파이프라인 실행
        self.scheduler._run_pipeline(self.schedule)
        
        # Pipeline 클래스 생성자 호출 확인
        mock_pipeline_class.assert_called_once_with(self.mock_config)
        
        # Pipeline.run 메소드 호출 확인
        mock_pipeline.run.assert_called_once()
        
        # 콜백 함수 호출 확인
        self.mock_callback.assert_called_once()
        
        # 실행 기록 저장 확인
        self.assertEqual(len(self.scheduler.completed_executions), 1)
        record = self.scheduler.completed_executions[0]
        self.assertEqual(record.status, "SUCCESS")
    
    def test_check_dependencies_with_no_dependencies(self):
        """의존성이 없는 경우"""
        self.schedule.dependencies = []
        
        result = self.scheduler._check_dependencies(self.schedule)
        
        self.assertTrue(result)
    
    def test_check_dependencies_with_fulfilled_dependencies(self):
        """충족된 의존성이 있는 경우"""
        # 의존성 파이프라인 ID
        dependency_id = "dependency_pipeline"
        
        # 의존성 설정
        self.schedule.dependencies = [dependency_id]
        
        # 성공적으로 완료된 의존성 파이프라인의 실행 기록 생성
        successful_record = ExecutionRecord("some_schedule", dependency_id)
        successful_record.complete(success=True)
        
        # 실행 기록 목록에 추가
        self.scheduler.completed_executions.append(successful_record)
        
        # 의존성 확인
        result = self.scheduler._check_dependencies(self.schedule)
        
        self.assertTrue(result)
    
    def test_check_dependencies_with_unfulfilled_dependencies(self):
        """충족되지 않은 의존성이 있는 경우"""
        # 의존성 파이프라인 ID
        dependency_id = "dependency_pipeline"
        
        # 의존성 설정
        self.schedule.dependencies = [dependency_id]
        
        # 실패한 의존성 파이프라인의 실행 기록 생성
        failed_record = ExecutionRecord("some_schedule", dependency_id)
        failed_record.complete(success=False, error_message="Error")
        
        # 실행 기록 목록에 추가
        self.scheduler.completed_executions.append(failed_record)
        
        # 의존성 확인
        result = self.scheduler._check_dependencies(self.schedule)
        
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main() 
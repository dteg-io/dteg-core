"""
오케스트레이터와 스케줄러 통합 테스트
"""
import os
import tempfile
import shutil
from pathlib import Path
from unittest import TestCase, mock

from dteg.orchestration.orchestrator import Orchestrator


class TestOrchestratorScheduler(TestCase):
    """오케스트레이터와 스케줄러 통합 테스트 클래스"""
    
    def setUp(self):
        """테스트 설정"""
        # 임시 디렉토리 생성
        self.temp_dir = tempfile.mkdtemp()
        self.test_schedule_dir = Path(self.temp_dir) / "schedules"
        self.test_history_dir = Path(self.temp_dir) / "history"
        self.test_result_dir = Path(self.temp_dir) / "results"
        
        self.test_schedule_dir.mkdir(parents=True, exist_ok=True)
        self.test_history_dir.mkdir(parents=True, exist_ok=True)
        self.test_result_dir.mkdir(parents=True, exist_ok=True)
        
        # 테스트 파이프라인 설정 파일 생성
        self.test_pipeline_file = Path(self.temp_dir) / "test-pipeline.yaml"
        with open(self.test_pipeline_file, 'w') as f:
            f.write("""
version: 1
pipeline:
  name: test-pipeline
  description: "테스트 파이프라인"
  source:
    type: dummy
  transformer:
    type: passthrough
  destination:
    type: dummy
""")
    
    def tearDown(self):
        """테스트 정리"""
        # 임시 디렉토리 삭제
        shutil.rmtree(self.temp_dir)
    
    def test_add_pipeline_saves_schedule(self):
        """파이프라인 추가 시 스케줄이 저장되는지 테스트"""
        # 오케스트레이터 생성
        orchestrator = Orchestrator(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir
        )
        
        # 파이프라인 추가
        pipeline_id = orchestrator.add_pipeline(
            pipeline_config=str(self.test_pipeline_file),
            cron_expression="0 8 * * *",
            enabled=True
        )
        
        # 스케줄 파일이 생성되었는지 확인
        schedule_file = self.test_schedule_dir / "schedules.json"
        self.assertTrue(schedule_file.exists())
        
        # 새 오케스트레이터 인스턴스 생성하여 로드 테스트
        orchestrator2 = Orchestrator(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir
        )
        
        # 스케줄이 로드되었는지 확인
        pipelines = orchestrator2.get_all_pipelines()
        self.assertEqual(len(pipelines), 1)
        
        loaded_pipeline = pipelines[0]
        self.assertEqual(loaded_pipeline["schedule_id"], pipeline_id)
        self.assertEqual(loaded_pipeline["cron_expression"], "0 8 * * *")
        self.assertTrue(loaded_pipeline["enabled"])
    
    def test_update_pipeline_saves_schedule(self):
        """파이프라인 업데이트 시 스케줄이 저장되는지 테스트"""
        # 오케스트레이터 생성
        orchestrator = Orchestrator(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir
        )
        
        # 파이프라인 추가
        pipeline_id = orchestrator.add_pipeline(
            pipeline_config=str(self.test_pipeline_file),
            cron_expression="0 8 * * *",
            enabled=True
        )
        
        # 파이프라인 업데이트
        orchestrator.update_pipeline(
            schedule_id=pipeline_id,
            cron_expression="0 12 * * *",
            enabled=False
        )
        
        # 새 오케스트레이터 인스턴스 생성하여 로드 테스트
        orchestrator2 = Orchestrator(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir
        )
        
        # 업데이트된 스케줄이 로드되었는지 확인
        pipelines = orchestrator2.get_all_pipelines()
        self.assertEqual(len(pipelines), 1)
        
        loaded_pipeline = pipelines[0]
        self.assertEqual(loaded_pipeline["schedule_id"], pipeline_id)
        self.assertEqual(loaded_pipeline["cron_expression"], "0 12 * * *")
        self.assertFalse(loaded_pipeline["enabled"])
    
    def test_remove_pipeline_removes_schedule(self):
        """파이프라인 삭제 시 스케줄이 삭제되는지 테스트"""
        # 오케스트레이터 생성
        orchestrator = Orchestrator(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir
        )
        
        # 두 개의 파이프라인 추가
        pipeline_id1 = orchestrator.add_pipeline(
            pipeline_config=str(self.test_pipeline_file),
            cron_expression="0 8 * * *",
            enabled=True
        )
        
        pipeline_id2 = orchestrator.add_pipeline(
            pipeline_config=str(self.test_pipeline_file),
            cron_expression="0 12 * * *",
            enabled=True
        )
        
        # 하나의 파이프라인 삭제
        orchestrator.remove_pipeline(pipeline_id1)
        
        # 새 오케스트레이터 인스턴스 생성하여 로드 테스트
        orchestrator2 = Orchestrator(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir
        )
        
        # 삭제된 스케줄이 로드되지 않았는지 확인
        pipelines = orchestrator2.get_all_pipelines()
        self.assertEqual(len(pipelines), 1)
        
        loaded_pipeline_ids = [p["schedule_id"] for p in pipelines]
        self.assertNotIn(pipeline_id1, loaded_pipeline_ids)
        self.assertIn(pipeline_id2, loaded_pipeline_ids)
    
    @mock.patch('dteg.orchestration.scheduler.Scheduler.run_once')
    def test_start_scheduler_uses_loaded_schedules(self, mock_run_once):
        """스케줄러 시작 시 로드된 스케줄을 사용하는지 테스트"""
        # 오케스트레이터 생성
        orchestrator = Orchestrator(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir
        )
        
        # 파이프라인 추가
        pipeline_id = orchestrator.add_pipeline(
            pipeline_config=str(self.test_pipeline_file),
            cron_expression="0 8 * * *",
            enabled=True
        )
        
        # 새 오케스트레이터 인스턴스 생성하여 로드 테스트
        orchestrator2 = Orchestrator(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir
        )
        
        # 스케줄러 시작
        orchestrator2.start_scheduler()
        
        # run_once가 호출되었는지 확인
        mock_run_once.assert_called()
        
        # 스케줄러 정지
        orchestrator2.stop_scheduler()
    
    def test_multiple_orchestrator_instances(self):
        """여러 오케스트레이터 인스턴스 간에 스케줄이 공유되는지 테스트"""
        # 첫 번째 오케스트레이터 생성 및 파이프라인 추가
        orchestrator1 = Orchestrator(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir
        )
        
        pipeline_id1 = orchestrator1.add_pipeline(
            pipeline_config=str(self.test_pipeline_file),
            cron_expression="0 8 * * *",
            enabled=True
        )
        
        # 두 번째 오케스트레이터 생성 및 파이프라인 추가
        orchestrator2 = Orchestrator(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir
        )
        
        pipeline_id2 = orchestrator2.add_pipeline(
            pipeline_config=str(self.test_pipeline_file),
            cron_expression="0 12 * * *",
            enabled=True
        )
        
        # 세 번째 오케스트레이터 생성 및 스케줄 확인
        orchestrator3 = Orchestrator(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir
        )
        
        pipelines = orchestrator3.get_all_pipelines()
        self.assertEqual(len(pipelines), 2)
        
        pipeline_ids = [p["schedule_id"] for p in pipelines]
        self.assertIn(pipeline_id1, pipeline_ids)
        self.assertIn(pipeline_id2, pipeline_ids)
    
    @mock.patch('dteg.orchestration.worker.CeleryTaskManager.run_pipeline')
    def test_run_pipeline_with_loaded_schedule(self, mock_run_pipeline):
        """로드된 스케줄로 파이프라인을 실행할 수 있는지 테스트"""
        # 첫 번째 오케스트레이터 생성 및 파이프라인 추가
        orchestrator1 = Orchestrator(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir,
            use_celery=True
        )
        
        pipeline_id = orchestrator1.add_pipeline(
            pipeline_config=str(self.test_pipeline_file),
            cron_expression="0 8 * * *",
            enabled=True
        )
        
        # 두 번째 오케스트레이터 생성 및 파이프라인 실행
        orchestrator2 = Orchestrator(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir,
            use_celery=True
        )
        
        # 태스크 ID 반환값 설정
        mock_run_pipeline.return_value = "task-123"
        
        # 파이프라인 실행 (비동기 모드로)
        result = orchestrator2.run_pipeline(
            pipeline_id=pipeline_id,
            async_execution=True
        )
        
        # run_pipeline이 호출되었는지 확인
        mock_run_pipeline.assert_called_once()
        
        # 결과 확인 (비동기 제출 성공)
        self.assertEqual(result["status"], "submitted")
        self.assertEqual(result["task_id"], "task-123")
        self.assertEqual(result["pipeline_id"], pipeline_id) 
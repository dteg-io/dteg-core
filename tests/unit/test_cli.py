"""
CLI 명령어 테스트
"""
import os
import json
import tempfile
import shutil
from pathlib import Path
from unittest import TestCase, mock

import pytest
from click.testing import CliRunner

from dteg.cli.main import cli, SCHEDULE_DIR, HISTORY_DIR, RESULT_DIR


class TestCLI(TestCase):
    """CLI 명령어 테스트 클래스"""
    
    def setUp(self):
        """테스트 설정"""
        self.runner = CliRunner()
        
        # 임시 디렉토리 생성
        self.temp_dir = tempfile.mkdtemp()
        self.test_schedule_dir = Path(self.temp_dir) / "schedules"
        self.test_history_dir = Path(self.temp_dir) / "history"
        self.test_result_dir = Path(self.temp_dir) / "results"
        
        self.test_schedule_dir.mkdir(parents=True, exist_ok=True)
        self.test_history_dir.mkdir(parents=True, exist_ok=True)
        self.test_result_dir.mkdir(parents=True, exist_ok=True)
        
        # 원래 디렉토리 경로 저장
        self.original_schedule_dir = SCHEDULE_DIR
        self.original_history_dir = HISTORY_DIR
        self.original_result_dir = RESULT_DIR
        
        # 테스트용 경로로 변경
        import dteg.cli.main as cli_main
        cli_main.SCHEDULE_DIR = self.test_schedule_dir
        cli_main.HISTORY_DIR = self.test_history_dir
        cli_main.RESULT_DIR = self.test_result_dir
        
        # 테스트 파이프라인 설정 파일 생성
        self.test_pipeline_file = Path(self.temp_dir) / "test-pipeline.yaml"
        with open(self.test_pipeline_file, 'w') as f:
            f.write("""
version: 1
pipeline:
  name: test-pipeline
  description: "테스트 파이프라인"
  
  source:
    type: csv
    config:
      file_path: "data/test.csv"
      delimiter: ","
      encoding: "utf-8"
      header: true
      
  transformer:
    type: sql
    config:
      engine: "sqlite"
      temp_table: "test_data"
      query: "SELECT * FROM test_data"
  
  destination:
    type: csv
    config:
      file_path: "data/output.csv"
      delimiter: ","
      encoding: "utf-8"
      header: true
""")
    
    def tearDown(self):
        """테스트 정리"""
        # 원래 경로로 복원
        import dteg.cli.main as cli_main
        cli_main.SCHEDULE_DIR = self.original_schedule_dir
        cli_main.HISTORY_DIR = self.original_history_dir
        cli_main.RESULT_DIR = self.original_result_dir
        
        # 임시 디렉토리 삭제
        shutil.rmtree(self.temp_dir)
    
    def test_cli_version(self):
        """CLI 버전 명령어 테스트"""
        result = self.runner.invoke(cli, ['--version'])
        assert result.exit_code == 0
        assert 'cli, version' in result.output
    
    @mock.patch('dteg.cli.main.get_orchestrator')
    def test_schedule_add_command(self, mock_get_orchestrator):
        """스케줄 추가 명령어 테스트"""
        # 오케스트레이터 목 설정
        mock_orchestrator = mock.MagicMock()
        mock_orchestrator.add_pipeline.return_value = "test-schedule-id"
        mock_get_orchestrator.return_value = mock_orchestrator
        
        # 명령어 실행
        result = self.runner.invoke(cli, [
            'schedule', 'add',
            str(self.test_pipeline_file),
            '--cron', '0 8 * * *',
            '--enabled'
        ])
        
        # 검증
        assert result.exit_code == 0
        assert '성공적으로 추가되었습니다' in result.output
        assert 'test-schedule-id' in result.output
        
        # 오케스트레이터 호출 검증
        mock_orchestrator.add_pipeline.assert_called_once_with(
            pipeline_config=str(self.test_pipeline_file),
            cron_expression='0 8 * * *',
            dependencies=None,
            enabled=True,
            max_retries=3,
            retry_delay=300
        )
    
    @mock.patch('dteg.cli.main.get_orchestrator')
    def test_schedule_list_command(self, mock_get_orchestrator):
        """스케줄 목록 조회 명령어 테스트"""
        # 오케스트레이터 목 설정
        mock_orchestrator = mock.MagicMock()
        mock_orchestrator.get_all_pipelines.return_value = [
            {
                "schedule_id": "test-schedule-id",
                "pipeline_id": "test-pipeline",
                "cron_expression": "0 8 * * *",
                "next_run": "2023-03-22T08:00:00",
                "enabled": True,
                "dependencies": []
            }
        ]
        mock_get_orchestrator.return_value = mock_orchestrator
        
        # 명령어 실행
        result = self.runner.invoke(cli, ['schedule', 'list'])
        
        # 검증
        assert result.exit_code == 0
        assert 'test-pipeline' in result.output
        assert '0 8 * * *' in result.output
        assert 'test-schedul' in result.output
        
        # 오케스트레이터 호출 검증
        mock_orchestrator.get_all_pipelines.assert_called_once()
    
    @mock.patch('dteg.cli.main.get_orchestrator')
    def test_schedule_update_command(self, mock_get_orchestrator):
        """스케줄 업데이트 명령어 테스트"""
        # 오케스트레이터 목 설정
        mock_orchestrator = mock.MagicMock()
        mock_orchestrator.update_pipeline.return_value = True
        mock_get_orchestrator.return_value = mock_orchestrator
        
        # 명령어 실행
        result = self.runner.invoke(cli, [
            'schedule', 'update',
            'test-schedule-id',
            '--cron', '0 12 * * *',
            '--enabled'
        ])
        
        # 검증
        assert result.exit_code == 0
        assert '성공적으로 업데이트되었습니다' in result.output
        
        # 오케스트레이터 호출 검증
        mock_orchestrator.update_pipeline.assert_called_once_with(
            'test-schedule-id',
            cron_expression='0 12 * * *',
            enabled=True
        )
    
    @mock.patch('dteg.cli.main.get_orchestrator')
    def test_schedule_delete_command(self, mock_get_orchestrator):
        """스케줄 삭제 명령어 테스트"""
        # 오케스트레이터 목 설정
        mock_orchestrator = mock.MagicMock()
        mock_orchestrator.remove_pipeline.return_value = True
        mock_get_orchestrator.return_value = mock_orchestrator
        
        # 명령어 실행 (--confirm 옵션으로 확인 절차 건너뛰기)
        result = self.runner.invoke(cli, [
            'schedule', 'delete',
            'test-schedule-id',
            '--confirm'
        ])
        
        # 검증
        assert result.exit_code == 0
        assert '성공적으로 삭제되었습니다' in result.output
        
        # 오케스트레이터 호출 검증
        mock_orchestrator.remove_pipeline.assert_called_once_with('test-schedule-id')
    
    @mock.patch('dteg.cli.main.get_orchestrator')
    def test_schedule_run_command(self, mock_get_orchestrator):
        """스케줄 실행 명령어 테스트"""
        # 오케스트레이터 목 설정
        mock_orchestrator = mock.MagicMock()
        mock_orchestrator.run_pipeline.return_value = {
            "execution_id": "test-execution-id",
            "status": "SUCCESS",
            "pipeline_id": "test-pipeline"
        }
        mock_get_orchestrator.return_value = mock_orchestrator
        
        # 명령어 실행
        result = self.runner.invoke(cli, [
            'schedule', 'run',
            'test-schedule-id'
        ])
        
        # 검증
        assert result.exit_code == 0
        assert '성공적으로 실행되었습니다' in result.output
        assert 'test-execution-id' in result.output
        
        # 오케스트레이터 호출 검증
        mock_orchestrator.run_pipeline.assert_called_once_with(
            pipeline_id='test-schedule-id',
            async_execution=False
        )
    
    @mock.patch('dteg.cli.main.get_orchestrator')
    @mock.patch('dteg.cli.main.time.sleep')
    def test_scheduler_start_command(self, mock_sleep, mock_get_orchestrator):
        """스케줄러 시작 명령어 테스트 (Ctrl+C 시뮬레이션)"""
        # 오케스트레이터 목 설정
        mock_orchestrator = mock.MagicMock()
        mock_get_orchestrator.return_value = mock_orchestrator
        
        # time.sleep이 호출되면 KeyboardInterrupt 발생시켜 루프 중단
        mock_sleep.side_effect = KeyboardInterrupt()
        
        # 명령어 실행
        result = self.runner.invoke(cli, [
            'scheduler', 'start',
            '--interval', '30'
        ])
        
        # 검증
        assert result.exit_code == 0
        assert '스케줄러가 시작되었습니다' in result.output
        assert '스케줄러가 중지되었습니다' in result.output
        
        # 오케스트레이터 호출 검증
        mock_orchestrator.start_scheduler.assert_called_once_with(interval=30)
        mock_orchestrator.stop_scheduler.assert_called_once()


class TestSchedulePersistence(TestCase):
    """스케줄 지속성 테스트 클래스"""
    
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
    
    def test_schedule_persistence(self):
        """스케줄 정보가 올바르게 저장되고 로드되는지 테스트"""
        from dteg.orchestration.scheduler import Scheduler, ScheduleConfig
        
        # 스케줄러 생성
        scheduler = Scheduler(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir
        )
        
        # 스케줄 추가
        schedule_config = ScheduleConfig(
            pipeline_config=str(self.test_pipeline_file),
            cron_expression="0 8 * * *",
            enabled=True
        )
        schedule_id = scheduler.add_schedule(schedule_config)
        
        # 스케줄 파일이 생성되었는지 확인
        schedule_file = self.test_schedule_dir / "schedules.json"
        assert schedule_file.exists()
        
        # 파일 내용 확인
        with open(schedule_file, 'r') as f:
            schedules_data = json.load(f)
        
        assert schedule_id in schedules_data
        assert schedules_data[schedule_id]["cron_expression"] == "0 8 * * *"
        assert schedules_data[schedule_id]["enabled"] == True
        
        # 새 스케줄러 인스턴스 생성하여 로드 테스트
        scheduler2 = Scheduler(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir
        )
        
        # 스케줄이 로드되었는지 확인
        loaded_schedules = scheduler2.get_all_schedules()
        assert len(loaded_schedules) == 1
        
        loaded_schedule = loaded_schedules[0]
        assert loaded_schedule.id == schedule_id
        assert loaded_schedule.cron_expression == "0 8 * * *"
        assert loaded_schedule.enabled == True


class TestCliIntegration(TestCase):
    """CLI 명령어 통합 테스트 클래스"""
    
    def setUp(self):
        """테스트 설정"""
        self.runner = CliRunner()
        
        # 임시 디렉토리 생성
        self.temp_dir = tempfile.mkdtemp()
        self.test_schedule_dir = Path(self.temp_dir) / "schedules"
        self.test_history_dir = Path(self.temp_dir) / "history"
        self.test_result_dir = Path(self.temp_dir) / "results"
        
        self.test_schedule_dir.mkdir(parents=True, exist_ok=True)
        self.test_history_dir.mkdir(parents=True, exist_ok=True)
        self.test_result_dir.mkdir(parents=True, exist_ok=True)
        
        # 원래 디렉토리 경로 저장
        self.original_schedule_dir = SCHEDULE_DIR
        self.original_history_dir = HISTORY_DIR
        self.original_result_dir = RESULT_DIR
        
        # 테스트용 경로로 변경
        import dteg.cli.main as cli_main
        cli_main.SCHEDULE_DIR = self.test_schedule_dir
        cli_main.HISTORY_DIR = self.test_history_dir
        cli_main.RESULT_DIR = self.test_result_dir
        
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
        # 원래 경로로 복원
        import dteg.cli.main as cli_main
        cli_main.SCHEDULE_DIR = self.original_schedule_dir
        cli_main.HISTORY_DIR = self.original_history_dir
        cli_main.RESULT_DIR = self.original_result_dir
        
        # 임시 디렉토리 삭제
        shutil.rmtree(self.temp_dir)
    
    @pytest.mark.skip(reason="실제 통합 테스트는 오래 걸릴 수 있어 기본적으로 스킵")
    def test_cli_integration_workflow(self):
        """전체 CLI 워크플로우 통합 테스트"""
        # 1. 스케줄 추가
        add_result = self.runner.invoke(cli, [
            'schedule', 'add',
            str(self.test_pipeline_file),
            '--cron', '0 8 * * *'
        ])
        assert add_result.exit_code == 0
        assert '성공적으로 추가되었습니다' in add_result.output
        
        # 스케줄 ID 추출
        import re
        schedule_id_match = re.search(r'스케줄 ID: \[cyan\](.+?)\[/\]', add_result.output)
        assert schedule_id_match
        schedule_id = schedule_id_match.group(1)
        
        # 2. 스케줄 목록 조회
        list_result = self.runner.invoke(cli, ['schedule', 'list'])
        assert list_result.exit_code == 0
        assert schedule_id in list_result.output
        assert 'test-pipeline' in list_result.output
        assert '0 8 * * *' in list_result.output
        
        # 3. 스케줄 업데이트
        update_result = self.runner.invoke(cli, [
            'schedule', 'update',
            schedule_id,
            '--cron', '0 12 * * *'
        ])
        assert update_result.exit_code == 0
        assert '성공적으로 업데이트되었습니다' in update_result.output
        
        # 업데이트 확인
        list_result_2 = self.runner.invoke(cli, ['schedule', 'list'])
        assert list_result_2.exit_code == 0
        assert '0 12 * * *' in list_result_2.output
        
        # 4. 스케줄 삭제
        delete_result = self.runner.invoke(cli, [
            'schedule', 'delete',
            schedule_id,
            '--confirm'
        ])
        assert delete_result.exit_code == 0
        assert '성공적으로 삭제되었습니다' in delete_result.output
        
        # 삭제 확인
        list_result_3 = self.runner.invoke(cli, ['schedule', 'list'])
        assert list_result_3.exit_code == 0
        assert '등록된 스케줄이 없습니다' in list_result_3.output 
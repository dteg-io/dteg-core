"""
스케줄러 지속성 테스트
"""
import json
import tempfile
import shutil
from pathlib import Path
from unittest import TestCase

from dteg.orchestration.scheduler import Scheduler, ScheduleConfig


class TestSchedulerPersistence(TestCase):
    """스케줄러 지속성 테스트 클래스"""
    
    def setUp(self):
        """테스트 설정"""
        # 임시 디렉토리 생성
        self.temp_dir = tempfile.mkdtemp()
        self.test_schedule_dir = Path(self.temp_dir) / "schedules"
        self.test_history_dir = Path(self.temp_dir) / "history"
        
        self.test_schedule_dir.mkdir(parents=True, exist_ok=True)
        self.test_history_dir.mkdir(parents=True, exist_ok=True)
        
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
    
    def test_schedule_save_load(self):
        """스케줄 정보가 올바르게 저장되고 로드되는지 테스트"""
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
        self.assertTrue(schedule_file.exists())
        
        # 파일 내용 확인
        with open(schedule_file, 'r') as f:
            schedules_data = json.load(f)
        
        self.assertIn(schedule_id, schedules_data)
        self.assertEqual(schedules_data[schedule_id]["cron_expression"], "0 8 * * *")
        self.assertEqual(schedules_data[schedule_id]["enabled"], True)
        
        # 새 스케줄러 인스턴스 생성하여 로드 테스트
        scheduler2 = Scheduler(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir
        )
        
        # 스케줄이 로드되었는지 확인
        loaded_schedules = scheduler2.get_all_schedules()
        self.assertEqual(len(loaded_schedules), 1)
        
        loaded_schedule = loaded_schedules[0]
        self.assertEqual(loaded_schedule.id, schedule_id)
        self.assertEqual(loaded_schedule.cron_expression, "0 8 * * *")
        self.assertTrue(loaded_schedule.enabled)
    
    def test_schedule_update_persistence(self):
        """스케줄 업데이트가 올바르게 저장되는지 테스트"""
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
        
        # 스케줄 업데이트
        updated = scheduler.update_schedule(
            schedule_id,
            cron_expression="0 12 * * *",
            enabled=False
        )
        self.assertTrue(updated)
        
        # 파일 내용 확인
        schedule_file = self.test_schedule_dir / "schedules.json"
        with open(schedule_file, 'r') as f:
            schedules_data = json.load(f)
        
        self.assertEqual(schedules_data[schedule_id]["cron_expression"], "0 12 * * *")
        self.assertEqual(schedules_data[schedule_id]["enabled"], False)
        
        # 새 스케줄러 인스턴스 생성하여 로드 테스트
        scheduler2 = Scheduler(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir
        )
        
        # 업데이트된 스케줄이 로드되었는지 확인
        loaded_schedules = scheduler2.get_all_schedules()
        loaded_schedule = next((s for s in loaded_schedules if s.id == schedule_id), None)
        self.assertIsNotNone(loaded_schedule)
        self.assertEqual(loaded_schedule.cron_expression, "0 12 * * *")
        self.assertFalse(loaded_schedule.enabled)
    
    def test_schedule_delete_persistence(self):
        """스케줄 삭제가 올바르게 저장되는지 테스트"""
        # 스케줄러 생성
        scheduler = Scheduler(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir
        )
        
        # 두 개의 스케줄 추가
        schedule_config1 = ScheduleConfig(
            pipeline_config=str(self.test_pipeline_file),
            cron_expression="0 8 * * *",
            enabled=True
        )
        schedule_id1 = scheduler.add_schedule(schedule_config1)
        
        schedule_config2 = ScheduleConfig(
            pipeline_config=str(self.test_pipeline_file),
            cron_expression="0 12 * * *",
            enabled=True
        )
        schedule_id2 = scheduler.add_schedule(schedule_config2)
        
        # 하나의 스케줄 삭제
        deleted = scheduler.remove_schedule(schedule_id1)
        self.assertTrue(deleted)
        
        # 파일 내용 확인
        schedule_file = self.test_schedule_dir / "schedules.json"
        with open(schedule_file, 'r') as f:
            schedules_data = json.load(f)
        
        self.assertNotIn(schedule_id1, schedules_data)
        self.assertIn(schedule_id2, schedules_data)
        
        # 새 스케줄러 인스턴스 생성하여 로드 테스트
        scheduler2 = Scheduler(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir
        )
        
        # 삭제된 스케줄이 로드되지 않았는지 확인
        loaded_schedules = scheduler2.get_all_schedules()
        self.assertEqual(len(loaded_schedules), 1)
        
        loaded_schedule_ids = [s.id for s in loaded_schedules]
        self.assertNotIn(schedule_id1, loaded_schedule_ids)
        self.assertIn(schedule_id2, loaded_schedule_ids)
    
    def test_schedule_to_dict_from_dict(self):
        """ScheduleConfig의 to_dict와 from_dict 메서드 테스트"""
        # 기본 ScheduleConfig 생성
        schedule_config = ScheduleConfig(
            pipeline_config=str(self.test_pipeline_file),
            cron_expression="0 8 * * *",
            enabled=True,
            max_retries=5,
            retry_delay=600,
            dependencies=["dependency1", "dependency2"]
        )
        
        # to_dict 메서드 테스트
        config_dict = schedule_config.to_dict()
        
        self.assertEqual(config_dict["pipeline_config"], str(self.test_pipeline_file))
        self.assertEqual(config_dict["cron_expression"], "0 8 * * *")
        self.assertEqual(config_dict["enabled"], True)
        self.assertEqual(config_dict["max_retries"], 5)
        self.assertEqual(config_dict["retry_delay"], 600)
        self.assertEqual(config_dict["dependencies"], ["dependency1", "dependency2"])
        
        # from_dict 메서드 테스트
        new_config = ScheduleConfig.from_dict(config_dict)
        
        self.assertEqual(new_config.pipeline_config, str(self.test_pipeline_file))
        self.assertEqual(new_config.cron_expression, "0 8 * * *")
        self.assertTrue(new_config.enabled)
        self.assertEqual(new_config.max_retries, 5)
        self.assertEqual(new_config.retry_delay, 600)
        self.assertEqual(new_config.dependencies, ["dependency1", "dependency2"])
    
    def test_load_invalid_schedule_file(self):
        """잘못된 형식의 스케줄 파일 로드 테스트"""
        # 잘못된 형식의 스케줄 파일 생성
        schedule_file = self.test_schedule_dir / "schedules.json"
        with open(schedule_file, 'w') as f:
            f.write("invalid json")
        
        # 스케줄러 생성 - 잘못된 파일이더라도 예외 없이 빈 스케줄로 초기화되어야 함
        scheduler = Scheduler(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir
        )
        
        # 로드된 스케줄이 없어야 함
        loaded_schedules = scheduler.get_all_schedules()
        self.assertEqual(len(loaded_schedules), 0)
    
    def test_multiple_schedules_persistence(self):
        """여러 스케줄이 올바르게 저장되고 로드되는지 테스트"""
        # 스케줄러 생성
        scheduler = Scheduler(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir
        )
        
        # 여러 스케줄 추가
        schedule_ids = []
        for i in range(5):
            schedule_config = ScheduleConfig(
                pipeline_config=str(self.test_pipeline_file),
                cron_expression=f"0 {i+8} * * *",
                enabled=i % 2 == 0  # 짝수 인덱스는 활성화, 홀수 인덱스는 비활성화
            )
            schedule_id = scheduler.add_schedule(schedule_config)
            schedule_ids.append(schedule_id)
        
        # 스케줄 파일이 생성되었는지 확인
        schedule_file = self.test_schedule_dir / "schedules.json"
        self.assertTrue(schedule_file.exists())
        
        # 파일 내용 확인
        with open(schedule_file, 'r') as f:
            schedules_data = json.load(f)
        
        self.assertEqual(len(schedules_data), 5)
        
        # 새 스케줄러 인스턴스 생성하여 로드 테스트
        scheduler2 = Scheduler(
            history_dir=self.test_history_dir,
            schedule_dir=self.test_schedule_dir
        )
        
        # 스케줄이 로드되었는지 확인
        loaded_schedules = scheduler2.get_all_schedules()
        self.assertEqual(len(loaded_schedules), 5)
        
        # 각 스케줄의 설정이 올바르게 로드되었는지 확인
        for i, schedule_id in enumerate(schedule_ids):
            loaded_schedule = next((s for s in loaded_schedules if s.id == schedule_id), None)
            self.assertIsNotNone(loaded_schedule)
            self.assertEqual(loaded_schedule.cron_expression, f"0 {i+8} * * *")
            self.assertEqual(loaded_schedule.enabled, i % 2 == 0) 
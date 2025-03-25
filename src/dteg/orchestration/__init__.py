"""
dteg.orchestration 패키지

파이프라인 스케줄링 및 작업 관리를 위한 모듈을 포함합니다.
"""

__version__ = "0.1.0"

from dteg.orchestration.orchestrator import Orchestrator

_orchestrator = None

def get_orchestrator(config=None, use_celery=False, broker_url=None, result_backend=None):
    """
    기본 오케스트레이터 인스턴스 반환
    
    기존 인스턴스가 없으면 새로 생성하고, 있으면 기존 인스턴스 반환
    
    Args:
        config: 오케스트레이터 설정
        use_celery: Celery 작업 큐 사용 여부
        broker_url: Celery 브로커 URL
        result_backend: Celery 결과 백엔드 URL
        
    Returns:
        Orchestrator: 오케스트레이터 인스턴스
    """
    global _orchestrator
    if _orchestrator is None:
        from dteg.orchestration.orchestrator import Orchestrator
        _orchestrator = Orchestrator(use_celery=use_celery, broker_url=broker_url, result_backend=result_backend)
        
        # 환경 변수에서 스케줄러 간격 가져오기 (기본값: 30초)
        import os
        scheduler_interval = int(os.environ.get("DTEG_SCHEDULER_INTERVAL", "30"))
        
        # 스케줄러 시작 (지정된 간격으로, 즉시 실행하지 않도록 설정)
        _orchestrator.start_scheduler(interval=scheduler_interval, no_immediate_run=True)
        
        # 웹 DB와 로컬 파일 통합을 위한 추가 기능
        def sync_schedules_with_web_db(self):
            """
            웹 SQLite DB와 로컬 스케줄러 파일 동기화
            """
            try:
                from dteg.web.database import SessionLocal
                from dteg.web.models.database_models import Schedule as WebSchedule
                
                # SQLite DB에서 스케줄 목록 가져오기
                db = SessionLocal()
                try:
                    web_schedules = db.query(WebSchedule).all()
                    
                    # 로컬 스케줄러의 스케줄 목록과 비교
                    local_schedules = self.scheduler.get_all_schedules()
                    local_schedule_ids = {s.id for s in local_schedules}
                    web_schedule_ids = {s.id for s in web_schedules}
                    
                    # 웹 DB에만 있고 로컬에 없는 스케줄 추가
                    for web_schedule in web_schedules:
                        if web_schedule.id not in local_schedule_ids:
                            if web_schedule.enabled:
                                self.schedule_pipeline(
                                    schedule_id=web_schedule.id,
                                    pipeline_id=web_schedule.pipeline_id,
                                    cron_expression=web_schedule.cron_expression,
                                    parameters=web_schedule.params
                                )
                    
                    # 로컬에만 있고 웹 DB에 없는 스케줄 제거
                    for local_schedule in local_schedules:
                        if local_schedule.id not in web_schedule_ids:
                            self.scheduler.remove_schedule(local_schedule.id)
                
                finally:
                    db.close()
                    
                return True
            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"웹 DB와 스케줄 동기화 실패: {str(e)}")
                return False
        
        # 스케줄러 오버라이드하여 스케줄 등록/제거 시 DB 동기화
        def schedule_pipeline(self, schedule_id, pipeline_id, cron_expression, parameters=None):
            """
            파이프라인 스케줄 등록
            
            Args:
                schedule_id: 스케줄 ID
                pipeline_id: 파이프라인 ID
                cron_expression: Cron 표현식
                parameters: 파이프라인 실행 매개변수
                
            Returns:
                str: 스케줄 ID
            """
            import json
            import os
            from datetime import datetime
            import uuid
            import os
            from pathlib import Path
            import logging
            import croniter
            
            logger = logging.getLogger(__name__)
            
            # 웹 API 호환 스케줄 파일 직접 생성
            try:
                # 기본 정보 설정
                now = datetime.now()
                
                # 다음 실행 시간 계산
                cron = croniter.croniter(cron_expression, now)
                next_run = cron.get_next(ret_type=datetime)
                
                # 스케줄 데이터 준비
                schedule_data = {
                    "id": schedule_id or str(uuid.uuid4()),
                    "name": f"Pipeline {pipeline_id[:8]}",
                    "description": f"Pipeline {pipeline_id[:8]}",
                    "pipeline_id": pipeline_id,
                    "cron_expression": cron_expression,
                    "enabled": True,
                    "parameters": parameters or {},
                    "params": parameters or {},
                    "created_at": now.isoformat(),
                    "updated_at": now.isoformat(),
                    "next_run": next_run.isoformat(),
                    "dependencies": [],
                    "max_retries": 3,
                    "retry_delay": 300
                }
                
                # 스케줄 파일 저장
                schedule_dir = self.scheduler.schedule_dir
                schedule_file = os.path.join(schedule_dir, f"{schedule_id}.json")
                
                with open(schedule_file, 'w') as f:
                    json.dump(schedule_data, f, indent=2)
                
                logger.info(f"스케줄 파일 생성됨: {schedule_file}")
                
                # 내부 스케줄러에 등록
                from dteg.orchestration.scheduler import ScheduleConfig
                
                # 스케줄 설정 생성
                schedule_config = ScheduleConfig(
                    pipeline_config=pipeline_id,  # 파이프라인 ID 직접 전달
                    cron_expression=cron_expression,
                    enabled=True
                )
                
                # ID를 직접 설정
                schedule_config.id = schedule_id
                
                # 스케줄 등록 (내부적으로만 사용되며 파일은 덮어쓰지 않음)
                return self.scheduler.add_schedule(schedule_config)
                
            except Exception as e:
                logger.error(f"스케줄 등록 중 오류 발생: {str(e)}")
                raise
        
        # 스케줄 삭제 메소드
        def remove_schedule(self, schedule_id):
            """
            스케줄 제거
            
            Args:
                schedule_id: 스케줄 ID
                
            Returns:
                bool: 제거 성공 여부
            """
            return self.scheduler.remove_schedule(schedule_id)
        
        # 동적으로 메서드 추가
        import types
        _orchestrator.schedule_pipeline = types.MethodType(schedule_pipeline, _orchestrator)
        _orchestrator.remove_schedule = types.MethodType(remove_schedule, _orchestrator)
        _orchestrator.sync_schedules_with_web_db = types.MethodType(sync_schedules_with_web_db, _orchestrator)
        
        # 초기 동기화 시도
        try:
            _orchestrator.sync_schedules_with_web_db()
        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f"초기 스케줄 동기화 실패: {str(e)}")
    
    return _orchestrator 
"""
dteg.orchestration 패키지

파이프라인 스케줄링 및 작업 관리를 위한 모듈을 포함합니다.
"""

__version__ = "0.1.0"

from dteg.orchestration.orchestrator import Orchestrator

_orchestrator = None

def get_orchestrator(config=None):
    """
    기본 오케스트레이터 인스턴스 반환
    
    기존 인스턴스가 없으면 새로 생성하고, 있으면 기존 인스턴스 반환
    
    Args:
        config: 오케스트레이터 설정
        
    Returns:
        Orchestrator: 오케스트레이터 인스턴스
    """
    global _orchestrator
    if _orchestrator is None:
        from dteg.orchestration.orchestrator import Orchestrator
        _orchestrator = Orchestrator()
        
        # 스케줄러 시작
        _orchestrator.start_scheduler()
        
        # 오케스트레이터 확장: schedule_pipeline 메서드 추가
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
            from dteg.orchestration.scheduler import ScheduleConfig
            
            # 스케줄 설정 생성
            schedule_config = ScheduleConfig(
                pipeline_config=pipeline_id,  # 파이프라인 ID 직접 전달
                cron_expression=cron_expression,
                id=schedule_id,  # 스케줄 ID 직접 설정
                enabled=True,
                name=schedule_id  # 이름은 스케줄 ID로 설정
            )
            
            # 스케줄 등록
            return self.scheduler.add_schedule(schedule_config)
        
        # 동적으로 메서드 추가
        import types
        _orchestrator.schedule_pipeline = types.MethodType(schedule_pipeline, _orchestrator)
        
    return _orchestrator 
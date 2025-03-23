#!/usr/bin/env python
"""
스케줄의 next_run 값을 업데이트하는 스크립트
"""
import sys
import os
from datetime import datetime

# 프로젝트 루트 디렉토리 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from dteg.web.database import get_db, engine
from dteg.web.models.database_models import Schedule
from sqlalchemy.orm import sessionmaker

def update_all_schedule_next_runs():
    """모든 스케줄의 next_run 값을 업데이트"""
    # 세션 생성
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # 모든 스케줄 가져오기
        schedules = session.query(Schedule).all()
        print(f"총 {len(schedules)}개의 스케줄을 찾았습니다.")
        
        for schedule in schedules:
            # 다음 실행 시간 계산
            next_run = schedule.calculate_next_run()
            if next_run:
                # 다음 실행 시간 업데이트
                schedule.next_run = next_run
                print(f"스케줄 ID: {schedule.id}, Cron: {schedule.cron_expression}, 다음 실행: {next_run}")
            else:
                print(f"스케줄 ID: {schedule.id}, Cron: {schedule.cron_expression}, 다음 실행 계산 실패")
        
        # 변경사항 저장
        session.commit()
        print("모든 스케줄 업데이트 완료")
    
    except Exception as e:
        print(f"오류 발생: {str(e)}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    print("스케줄 next_run 값 업데이트 시작...")
    update_all_schedule_next_runs()
    print("스케줄 next_run 값 업데이트 완료") 
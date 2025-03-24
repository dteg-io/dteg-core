#!/usr/bin/env python
"""
실행 이력 마이그레이션 스크립트

JSON 파일로 저장된 실행 이력을 SQLite 데이터베이스로 마이그레이션
"""
import os
import json
import logging
from datetime import datetime
from pathlib import Path
import sys

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def migrate_execution_history():
    """
    실행 이력 JSON 파일을 SQLite 데이터베이스로 마이그레이션
    """
    try:
        # 데이터베이스 모듈 가져오기
        from dteg.web.database import SessionLocal
        from dteg.web.models.database_models import Execution as DBExecution
        
        # 이력 디렉토리 (기본값: ~/.dteg/history)
        history_dir = Path.home() / ".dteg" / "history"
        
        if not history_dir.exists():
            logger.info(f"이력 디렉토리가 존재하지 않습니다: {history_dir}")
            return
        
        # 데이터베이스 연결
        db = SessionLocal()
        try:
            # 이력 파일 목록 가져오기
            execution_files = list(history_dir.glob("*.json"))
            logger.info(f"{len(execution_files)}개의 실행 이력 파일을 발견했습니다.")
            
            migrated_count = 0
            skipped_count = 0
            
            # 각 이력 파일 처리
            for file_path in execution_files:
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # 이미 데이터베이스에 존재하는지 확인
                    execution_id = data.get("id")
                    if not execution_id:
                        logger.warning(f"ID가 없는 실행 이력 건너뛰기: {file_path}")
                        skipped_count += 1
                        continue
                    
                    existing = db.query(DBExecution).filter(DBExecution.id == execution_id).first()
                    if existing:
                        logger.debug(f"이미 존재하는 실행 이력 건너뛰기: {execution_id}")
                        skipped_count += 1
                        continue
                    
                    # 실행 이력 데이터 준비
                    start_time = datetime.fromisoformat(data["start_time"]) if data.get("start_time") else None
                    end_time = datetime.fromisoformat(data["end_time"]) if data.get("end_time") else None
                    
                    # 로그 데이터 처리
                    logs = None
                    if data.get("logs"):
                        if isinstance(data["logs"], list):
                            logs = '\n'.join(data["logs"])
                        elif isinstance(data["logs"], str):
                            logs = data["logs"]
                    
                    # 상태 매핑
                    status_map = {
                        "RUNNING": "running",
                        "SUCCESS": "completed",
                        "FAILED": "failed",
                        "RETRYING": "running"
                    }
                    status = status_map.get(data.get("status", ""), "unknown")
                    
                    # 데이터베이스 객체 생성
                    db_execution = DBExecution(
                        id=execution_id,
                        pipeline_id=data.get("pipeline_id", "unknown"),
                        schedule_id=data.get("schedule_id", None),
                        status=status,
                        started_at=start_time,
                        ended_at=end_time,
                        error_message=data.get("error_message"),
                        trigger=data.get("trigger", "manual"),
                        logs=logs
                    )
                    
                    # 데이터베이스에 추가
                    db.add(db_execution)
                    db.commit()
                    migrated_count += 1
                    logger.debug(f"실행 이력 마이그레이션 성공: {execution_id}")
                    
                except Exception as e:
                    logger.error(f"실행 이력 마이그레이션 실패 ({file_path}): {str(e)}")
                    db.rollback()
                    skipped_count += 1
            
            logger.info(f"마이그레이션 완료: {migrated_count}개 성공, {skipped_count}개 건너뜀")
            
        finally:
            db.close()
    
    except Exception as e:
        logger.error(f"마이그레이션 프로세스 실패: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    logger.info("실행 이력 마이그레이션 시작...")
    success = migrate_execution_history()
    logger.info("실행 이력 마이그레이션 완료")
    sys.exit(0 if success else 1) 
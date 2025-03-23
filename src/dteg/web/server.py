"""
DTEG Web Server

웹 서버 실행 모듈
"""
import os
import argparse
from pathlib import Path

import uvicorn
from dteg.utils.logging import configure_logging, get_logger
from dteg.web.database import init_db


def run_server(host="0.0.0.0", port=8000, reload=False, log_level="info"):
    """
    웹 서버 실행
    
    Args:
        host (str): 바인딩할 호스트 주소
        port (int): 웹 서버 포트
        reload (bool): 코드 변경 시 자동 재로딩 여부
        log_level (str): 로깅 레벨 (debug, info, warning, error, critical)
    """
    # 로깅 설정
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"web_server_{port}.log"
    
    configure_logging(
        level=log_level.upper(),
        log_file=str(log_file)
    )
    
    logger = get_logger()
    logger.info(f"웹 서버 시작 (host={host}, port={port}, reload={reload})")
    
    # 데이터베이스 초기화
    try:
        init_db()
        logger.info("데이터베이스 초기화 완료")
    except Exception as e:
        logger.error(f"데이터베이스 초기화 오류: {str(e)}")
    
    # 서버 실행 설정
    uvicorn.run(
        "dteg.web.api.main:app",
        host=host,
        port=port,
        reload=reload,
        log_level=log_level.lower()
    )


def main():
    """명령행 인터페이스"""
    parser = argparse.ArgumentParser(description='DTEG 웹 서버')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='서버 호스트')
    parser.add_argument('--port', type=int, default=8000, help='서버 포트')
    parser.add_argument('--reload', action='store_true', help='코드 변경 시 자동 재로딩')
    parser.add_argument('--log-level', type=str, default='info', help='로깅 레벨')
    
    args = parser.parse_args()
    run_server(host=args.host, port=args.port, reload=args.reload, log_level=args.log_level)


if __name__ == "__main__":
    main() 
"""
로깅 시스템 구현 모듈
"""
import logging
import os
import sys
from datetime import datetime
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler

# 로그 레벨 매핑
LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

# 전역 콘솔 객체
console = Console()

# 기본 로거 설정
logger = logging.getLogger("dteg")


def configure_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    log_dir: Optional[str] = None
) -> None:
    """로깅 시스템 설정

    Args:
        level: 로그 레벨 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: 로그 파일 이름 (기본값: None, 설정 시 파일에 로그 저장)
        log_dir: 로그 파일 디렉토리 (기본값: None, 지정 시 log_file과 함께 사용)
    """
    # 로그 레벨 설정
    log_level = LOG_LEVELS.get(level.upper(), logging.INFO)
    logger.setLevel(log_level)

    # 모든 핸들러 제거 (재설정 시 중복 방지)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # 콘솔 로그 핸들러 설정 (Rich 활용)
    console_handler = RichHandler(
        rich_tracebacks=True,
        show_time=False,
        show_path=False,
        markup=True,
    )
    console_handler.setLevel(log_level)
    logger.addHandler(console_handler)

    # 파일 로그 핸들러 설정 (지정된 경우)
    if log_file:
        try:
            if log_dir:
                os.makedirs(log_dir, exist_ok=True)
                log_path = os.path.join(log_dir, log_file)
            else:
                log_path = log_file

            # 타임스탬프를 파일명에 추가
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            file_name, file_ext = os.path.splitext(log_path)
            log_file_path = f"{file_name}_{timestamp}{file_ext}"

            file_handler = logging.FileHandler(
                log_file_path, mode="a", encoding="utf-8"
            )
            
            # 파일 로그 포맷 설정
            file_format = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            file_handler.setFormatter(file_format)
            file_handler.setLevel(log_level)
            logger.addHandler(file_handler)
        except Exception as e:
            console.print(f"[bold red]로그 파일 생성 중 오류 발생: {e}[/]")
            sys.exit(1)

    # 로깅 설정 완료 로그
    logger.debug("로깅 시스템이 설정되었습니다.")


def get_logger() -> logging.Logger:
    """DTEG 로거 반환

    Returns:
        설정된 로거 인스턴스
    """
    return logger 
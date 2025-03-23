"""
Rich 라이브러리를 활용한 유틸리티 함수

콘솔 출력 관련 유틸리티 함수 모음
"""
import sys
import time
import contextlib
from typing import Optional, Callable, Any

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()

@contextlib.contextmanager
def spinner(text: str, callback: Optional[Callable[[], Any]] = None):
    """
    Rich 라이브러리를 활용한 스피너 표시 컨텍스트 매니저
    
    Args:
        text: 표시할 텍스트
        callback: 스피너 종료 시 호출할 콜백 함수 (선택적)
    """
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        transient=True,
        console=console
    ) as progress:
        task_id = progress.add_task(text, total=None)
        try:
            yield
        finally:
            progress.remove_task(task_id)
            if callback:
                callback() 
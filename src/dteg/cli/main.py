"""
DTEG 명령줄 인터페이스
"""
import sys
from typing import Optional

import click
from rich.console import Console

from dteg import __version__

console = Console()


@click.group()
@click.version_option(version=__version__)
def cli() -> None:
    """DTEG: 데이터 파이프라인 구축을 위한 확장 가능한 ETL 도구"""
    pass


@cli.command()
@click.argument("project_dir", required=False)
def init(project_dir: Optional[str] = None) -> None:
    """새 DTEG 프로젝트 초기화"""
    if project_dir:
        console.print(f"[bold green]프로젝트 디렉토리[/] [cyan]{project_dir}[/] 에 새 DTEG 프로젝트를 초기화합니다.")
    else:
        console.print("[bold green]현재 디렉토리에 새 DTEG 프로젝트를 초기화합니다.[/]")
    
    # TODO: 프로젝트 템플릿 생성 로직 구현
    console.print("[bold green]✓[/] 프로젝트가 성공적으로 초기화되었습니다!")


@cli.command()
@click.argument("config_file", required=True, type=click.Path(exists=True))
@click.option("--verbose", "-v", is_flag=True, help="상세 로그 출력")
def run(config_file: str, verbose: bool) -> None:
    """YAML 설정 파일로 파이프라인 실행"""
    console.print(f"[bold green]설정 파일[/] [cyan]{config_file}[/] 로 파이프라인을 실행합니다.")
    if verbose:
        console.print("[bold yellow]상세 모드가 활성화되었습니다.[/]")
    
    # TODO: 파이프라인 실행 로직 구현
    console.print("[bold green]✓[/] 파이프라인이 성공적으로 완료되었습니다!")


@cli.command()
def info() -> None:
    """DTEG 및 시스템 정보 표시"""
    console.print("[bold]DTEG 정보[/]")
    console.print(f"버전: [cyan]{__version__}[/]")
    console.print(f"Python: [cyan]{sys.version.split()[0]}[/]")
    console.print(f"플랫폼: [cyan]{sys.platform}[/]")
    
    # TODO: 설치된 플러그인 및 추가 정보 표시


if __name__ == "__main__":
    cli() 
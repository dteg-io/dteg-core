"""
DTEG 명령줄 인터페이스
"""
import os
import sys
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from dteg import __version__
from dteg.core.config import generate_default_config, load_config
from dteg.core.context import ExecutionStatus
from dteg.core.pipeline import Pipeline
from dteg.core.plugin import discover_plugins
from dteg.utils.logging import configure_logging, get_logger

# 콘솔 및 로거 초기화
console = Console()
logger = get_logger()


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
        project_path = Path(project_dir)
        console.print(f"[bold green]프로젝트 디렉토리[/] [cyan]{project_dir}[/] 에 새 DTEG 프로젝트를 초기화합니다.")
        project_path.mkdir(exist_ok=True, parents=True)
    else:
        project_path = Path.cwd()
        console.print("[bold green]현재 디렉토리에 새 DTEG 프로젝트를 초기화합니다.[/]")
    
    # 디렉토리 구조 생성
    (project_path / "pipelines").mkdir(exist_ok=True)
    (project_path / "logs").mkdir(exist_ok=True)
    (project_path / "data").mkdir(exist_ok=True)
    (project_path / "scripts").mkdir(exist_ok=True)
    
    # 기본 파이프라인 설정 파일 생성
    pipeline_config = generate_default_config()
    config_path = project_path / "pipelines" / "default-pipeline.yaml"
    
    with open(config_path, "w", encoding="utf-8") as f:
        import yaml
        yaml.dump(pipeline_config, f, default_flow_style=False, sort_keys=False)
    
    # README 파일 생성
    readme_path = project_path / "README.md"
    with open(readme_path, "w", encoding="utf-8") as f:
        f.write(f"""# DTEG 프로젝트

데이터 파이프라인 프로젝트

## 디렉토리 구조

- `pipelines/`: 파이프라인 설정 파일
- `logs/`: 로그 파일
- `data/`: 데이터 파일
- `scripts/`: 유틸리티 스크립트

## 시작하기

파이프라인 실행하기:

```bash
dteg run pipelines/default-pipeline.yaml
```

""")
    
    console.print("[bold green]✓[/] 프로젝트가 성공적으로 초기화되었습니다!")
    console.print(f"기본 파이프라인 설정: [cyan]{config_path}[/]")


@cli.command()
@click.argument("config_file", required=True, type=click.Path(exists=True))
@click.option("--verbose", "-v", is_flag=True, help="상세 로그 출력")
@click.option("--batch", "-b", is_flag=True, help="배치 처리 모드 활성화")
@click.option("--batch-size", type=int, help="배치 크기 지정")
@click.option("--log-file", type=str, help="로그 파일 경로")
@click.option("--log-level", type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]), default="INFO", help="로그 레벨")
@click.option("--validate-only", is_flag=True, help="설정만 검증하고 실행하지 않음")
def run(
    config_file: str, 
    verbose: bool,
    batch: bool,
    batch_size: Optional[int],
    log_file: Optional[str],
    log_level: str,
    validate_only: bool
) -> None:
    """YAML 설정 파일로 파이프라인 실행"""
    # 로깅 설정
    log_level_actual = "DEBUG" if verbose else log_level
    log_dir = os.path.join(os.path.dirname(os.path.abspath(config_file)), "..", "logs")
    
    configure_logging(
        level=log_level_actual,
        log_file=log_file if log_file else "pipeline.log",
        log_dir=log_dir if not log_file else None
    )
    
    console.print(f"[bold green]설정 파일[/] [cyan]{config_file}[/] 로 파이프라인을 실행합니다.")
    
    try:
        # 파이프라인 인스턴스 생성
        pipeline = Pipeline(config_file)
        
        # 검증 모드
        if validate_only:
            console.print("[bold yellow]설정 검증 모드가 활성화되었습니다.[/]")
            if pipeline.validate():
                console.print("[bold green]✓[/] 파이프라인 설정이 유효합니다.")
                return
            else:
                console.print("[bold red]✗[/] 파이프라인 설정이 유효하지 않습니다.")
                sys.exit(1)
        
        # 배치 처리 모드
        if batch:
            console.print("[bold yellow]배치 처리 모드가 활성화되었습니다.[/]")
            status, context = pipeline.run_batch(batch_size)
        else:
            # 일반 모드
            status, context = pipeline.run()
        
        # 결과 출력
        if status == ExecutionStatus.SUCCEEDED:
            console.print(f"[bold green]✓[/] 파이프라인이 성공적으로 완료되었습니다!")
            
            # 메트릭 출력
            metrics = context.metrics.to_dict()
            table = Table(title="파이프라인 실행 결과")
            
            table.add_column("지표", style="cyan")
            table.add_column("값", style="green")
            
            table.add_row("파이프라인", context.pipeline_name)
            table.add_row("실행 ID", context.run_id)
            table.add_row("상태", status.value)
            
            if metrics.get("execution_time_seconds"):
                table.add_row("실행 시간", f"{metrics['execution_time_seconds']:.2f}초")
            
            table.add_row("처리된 행 수", str(metrics["rows_processed"]))
            
            console.print(table)
        else:
            console.print(f"[bold red]✗[/] 파이프라인 실행이 실패했습니다: {status.value}")
            
            if "error" in context.metadata:
                console.print(f"[bold red]오류:[/] {context.metadata['error']}")
            
            sys.exit(1)
            
    except Exception as e:
        console.print(f"[bold red]✗[/] 파이프라인 실행 중 오류가 발생했습니다: {str(e)}")
        if verbose:
            import traceback
            console.print(traceback.format_exc())
        sys.exit(1)


@cli.command()
def info() -> None:
    """DTEG 및 시스템 정보 표시"""
    console.print(Panel.fit("[bold]DTEG 정보[/]", border_style="green"))
    
    table = Table()
    table.add_column("항목", style="cyan")
    table.add_column("값", style="green")
    
    table.add_row("버전", __version__)
    table.add_row("Python", sys.version.split()[0])
    table.add_row("플랫폼", sys.platform)
    
    # 플러그인 정보 표시
    discover_plugins()
    from dteg.core.plugin import PluginRegistry
    
    extractors = PluginRegistry.list_extractors()
    loaders = PluginRegistry.list_loaders()
    transformers = PluginRegistry.list_transformers()
    
    table.add_row("Extractors", ", ".join(extractors) if extractors else "없음")
    table.add_row("Loaders", ", ".join(loaders) if loaders else "없음")
    table.add_row("Transformers", ", ".join(transformers) if transformers else "없음")
    
    console.print(table)


@cli.command()
@click.argument("config_file", required=True, type=click.Path(exists=True))
def validate(config_file: str) -> None:
    """파이프라인 설정 파일 검증"""
    console.print(f"[bold green]설정 파일[/] [cyan]{config_file}[/] 를 검증합니다.")
    
    try:
        # 설정 파일 로드
        config = load_config(config_file)
        console.print("[bold green]✓[/] 설정 파일 스키마가 유효합니다.")
        
        # 파이프라인 유효성 검사
        pipeline = Pipeline(config)
        if pipeline.validate():
            console.print("[bold green]✓[/] 파이프라인 구성이 유효합니다.")
        else:
            console.print("[bold red]✗[/] 파이프라인 구성이 유효하지 않습니다.")
            sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]✗[/] 설정 파일 검증 중 오류 발생: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    cli() 
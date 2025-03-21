"""
DTEG 명령줄 인터페이스
"""
import os
import sys
from pathlib import Path
from typing import Optional, List
import datetime
import time

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
from dteg.orchestration.orchestrator import Orchestrator

# 콘솔 및 로거 초기화
console = Console()
logger = get_logger()

# 기본 디렉토리 설정
DEFAULT_CONFIG_DIR = Path.home() / ".dteg"
SCHEDULE_DIR = DEFAULT_CONFIG_DIR / "schedules"
HISTORY_DIR = DEFAULT_CONFIG_DIR / "history"
RESULT_DIR = DEFAULT_CONFIG_DIR / "results"

# 디렉토리 생성
DEFAULT_CONFIG_DIR.mkdir(exist_ok=True, parents=True)
SCHEDULE_DIR.mkdir(exist_ok=True, parents=True)
HISTORY_DIR.mkdir(exist_ok=True, parents=True)
RESULT_DIR.mkdir(exist_ok=True, parents=True)

# 오케스트레이터 인스턴스를 초기화하는 함수
def get_orchestrator(use_celery=False, broker_url=None, result_backend=None):
    return Orchestrator(
        history_dir=HISTORY_DIR,
        result_dir=RESULT_DIR,
        schedule_dir=SCHEDULE_DIR,
        broker_url=broker_url,
        result_backend=result_backend,
        use_celery=use_celery
    )

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
@click.option("--use-celery", is_flag=True, help="Celery 작업 큐 사용 정보 표시")
def info(use_celery: bool = False) -> None:
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
    
    # 스케줄 및 오케스트레이션 정보 표시
    try:
        orchestrator = get_orchestrator(use_celery=use_celery)
        schedules = orchestrator.get_all_pipelines()
        table.add_row("등록된 스케줄", str(len(schedules)))
        
        # 활성화된 스케줄 수 계산
        active_schedules = sum(1 for s in schedules if s.get('enabled', False))
        table.add_row("활성화된 스케줄", str(active_schedules))
        
        # 스케줄러 상태 확인
        scheduler_status = "실행 중" if orchestrator.scheduler_running else "중지됨"
        table.add_row("스케줄러 상태", scheduler_status)
        
        # Celery 작업 큐 정보 표시
        if orchestrator.use_celery:
            from dteg.orchestration.worker import get_broker_url, get_result_backend
            broker_url = get_broker_url()
            result_backend = get_result_backend()
            
            # 브로커 유형 표시 (Redis, RabbitMQ 등)
            broker_type = "Redis"
            if "rabbitmq" in broker_url.lower():
                broker_type = "RabbitMQ"
            elif "amqp" in broker_url.lower():
                broker_type = "AMQP"
            elif "sqs" in broker_url.lower():
                broker_type = "SQS"
                
            table.add_row("작업 큐 사용", "Celery")
            table.add_row("브로커 유형", broker_type)
            
            # 실행 중인 작업 정보
            try:
                running_tasks = len(orchestrator.task_manager.get_active_tasks())
                table.add_row("실행 중인 작업", str(running_tasks))
            except:
                table.add_row("실행 중인 작업", "조회 불가")
        else:
            table.add_row("작업 큐 사용", "사용 안 함")
    
    except Exception as e:
        table.add_row("스케줄 정보", f"조회 실패: {str(e)}")
    
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


@cli.group()
def schedule():
    """파이프라인 스케줄 관리 명령어"""
    pass


@schedule.command("add")
@click.argument("pipeline_config", required=True, type=click.Path(exists=True))
@click.option("--cron", "-c", required=True, help="Cron 표현식 (예: '0 8 * * *' - 매일 오전 8시)")
@click.option("--enabled/--disabled", default=True, help="스케줄 활성화 여부")
@click.option("--max-retries", type=int, default=3, help="실패 시 최대 재시도 횟수")
@click.option("--retry-delay", type=int, default=300, help="재시도 간격(초)")
@click.option("--dependency", "-d", multiple=True, help="의존성 있는 파이프라인 ID (여러 개 가능)")
def add_schedule(
    pipeline_config: str,
    cron: str,
    enabled: bool,
    max_retries: int,
    retry_delay: int,
    dependency: List[str]
):
    """파이프라인 스케줄 추가"""
    try:
        orchestrator = get_orchestrator()
        schedule_id = orchestrator.add_pipeline(
            pipeline_config=pipeline_config,
            cron_expression=cron,
            dependencies=list(dependency) if dependency else None,
            enabled=enabled,
            max_retries=max_retries,
            retry_delay=retry_delay
        )
        console.print(f"[bold green]✓[/] 스케줄이 성공적으로 추가되었습니다. 스케줄 ID: [cyan]{schedule_id}[/]")
    except Exception as e:
        console.print(f"[bold red]✗[/] 스케줄 추가 중 오류 발생: {str(e)}")
        sys.exit(1)


@schedule.command("list")
def list_schedules():
    """등록된 모든 파이프라인 스케줄 조회"""
    try:
        orchestrator = get_orchestrator()
        pipelines = orchestrator.get_all_pipelines()
        
        if not pipelines:
            console.print("[yellow]등록된 스케줄이 없습니다.[/]")
            return
            
        table = Table(title="등록된 파이프라인 스케줄")
        table.add_column("스케줄 ID", style="cyan")
        table.add_column("파이프라인 ID", style="green")
        table.add_column("Cron 표현식", style="blue")
        table.add_column("다음 실행 시간", style="magenta")
        table.add_column("활성화", style="yellow")
        table.add_column("의존성", style="red")
        
        for pipeline in pipelines:
            table.add_row(
                pipeline["schedule_id"],
                pipeline["pipeline_id"],
                pipeline["cron_expression"],
                pipeline["next_run"],
                "✓" if pipeline["enabled"] else "✗",
                ", ".join(pipeline["dependencies"]) if pipeline["dependencies"] else "없음"
            )
            
        console.print(table)
    except Exception as e:
        console.print(f"[bold red]✗[/] 스케줄 목록 조회 중 오류 발생: {str(e)}")
        sys.exit(1)


@schedule.command("update")
@click.argument("schedule_id", required=True)
@click.option("--cron", "-c", help="Cron 표현식 업데이트")
@click.option("--enabled/--disabled", default=None, help="스케줄 활성화 여부")
@click.option("--max-retries", type=int, help="실패 시 최대 재시도 횟수")
def update_schedule(schedule_id: str, cron: Optional[str], enabled: Optional[bool], max_retries: Optional[int]):
    """파이프라인 스케줄 설정 업데이트"""
    try:
        orchestrator = get_orchestrator()
        # 업데이트할 속성만 전달
        update_args = {}
        if cron is not None:
            update_args["cron_expression"] = cron
        if enabled is not None:
            update_args["enabled"] = enabled
        if max_retries is not None:
            update_args["max_retries"] = max_retries
            
        if not update_args:
            console.print("[yellow]업데이트할 속성이 지정되지 않았습니다.[/]")
            return
            
        success = orchestrator.update_pipeline(schedule_id, **update_args)
        
        if success:
            console.print(f"[bold green]✓[/] 스케줄 [cyan]{schedule_id}[/]가 성공적으로 업데이트되었습니다.")
        else:
            console.print(f"[bold red]✗[/] 스케줄 [cyan]{schedule_id}[/]를 찾을 수 없습니다.")
            sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]✗[/] 스케줄 업데이트 중 오류 발생: {str(e)}")
        sys.exit(1)


@schedule.command("delete")
@click.argument("schedule_id", required=True)
@click.option("--confirm", is_flag=True, help="확인 없이 삭제")
def delete_schedule(schedule_id: str, confirm: bool):
    """파이프라인 스케줄 삭제"""
    try:
        orchestrator = get_orchestrator()
        if not confirm:
            if not click.confirm(f"스케줄 ID {schedule_id}를 삭제하시겠습니까?"):
                console.print("[yellow]삭제가 취소되었습니다.[/]")
                return
                
        success = orchestrator.remove_pipeline(schedule_id)
        
        if success:
            console.print(f"[bold green]✓[/] 스케줄 [cyan]{schedule_id}[/]가 성공적으로 삭제되었습니다.")
        else:
            console.print(f"[bold red]✗[/] 스케줄 [cyan]{schedule_id}[/]를 찾을 수 없습니다.")
            sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]✗[/] 스케줄 삭제 중 오류 발생: {str(e)}")
        sys.exit(1)


@schedule.command("run")
@click.argument("schedule_id", required=True)
@click.option("--async", "async_mode", is_flag=True, help="비동기 모드로 실행")
def run_schedule(schedule_id: str, async_mode: bool):
    """파이프라인 스케줄 즉시 실행"""
    try:
        orchestrator = get_orchestrator(use_celery=async_mode)
        result = orchestrator.run_pipeline(
            pipeline_id=schedule_id,
            async_execution=async_mode
        )
        
        if result["status"] == "submitted":
            console.print(f"[bold green]✓[/] 파이프라인이 비동기 모드로 제출되었습니다.")
            console.print(f"  실행 ID: [cyan]{result['execution_id']}[/]")
            console.print(f"  작업 ID: [cyan]{result.get('task_id')}[/]")
        elif result["status"] == "SUCCESS":
            console.print(f"[bold green]✓[/] 파이프라인이 성공적으로 실행되었습니다.")
            console.print(f"  실행 ID: [cyan]{result['execution_id']}[/]")
        else:
            console.print(f"[bold red]✗[/] 파이프라인 실행 실패: {result['status']}")
            if result.get("error_message"):
                console.print(f"  오류: {result['error_message']}")
            sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]✗[/] 스케줄 실행 중 오류 발생: {str(e)}")
        sys.exit(1)


@cli.group()
def scheduler():
    """스케줄러 관리 명령어"""
    pass


@scheduler.command("start")
@click.option("--interval", type=int, default=60, help="스케줄 확인 간격(초)")
@click.option("--daemon", is_flag=True, help="데몬 모드로 실행 (백그라운드)")
@click.option("--use-celery", is_flag=True, help="Celery 작업 큐 사용")
@click.option("--broker-url", help="Celery 브로커 URL (기본값: CELERY_BROKER_URL 환경 변수)")
@click.option("--result-backend", help="Celery 결과 백엔드 URL (기본값: CELERY_RESULT_BACKEND 환경 변수)")
@click.option("--verbose", "-v", is_flag=True, help="자세한 로그 출력")
@click.option("--log-level", type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]), default=None, help="로그 레벨 설정")
def start_scheduler(interval: int, daemon: bool, use_celery: bool, broker_url: Optional[str], result_backend: Optional[str], verbose: bool, log_level: Optional[str]):
    """스케줄러 시작"""
    try:
        if daemon:
            console.print("[yellow]데몬 모드는 아직 구현되지 않았습니다. 포그라운드 모드로 실행합니다.[/]")
        
        # 로그 레벨 조정
        if verbose or log_level:
            import logging
            # verbose는 항상 DEBUG 레벨, log_level이 지정되면 해당 레벨 사용
            selected_level = logging.DEBUG if verbose else (getattr(logging, log_level) if log_level else logging.INFO)
            logging.getLogger('dteg').setLevel(selected_level)
            console.print(f"[yellow]로그 레벨이 {logging.getLevelName(selected_level)}로 설정되었습니다.[/]")
            
        orchestrator = get_orchestrator(use_celery=use_celery, broker_url=broker_url, result_backend=result_backend)
        
        # 등록된 스케줄 정보 표시
        schedules = orchestrator.get_all_pipelines()
        if schedules:
            table = Table(title="실행할 스케줄 목록")
            table.add_column("ID", style="cyan")
            table.add_column("파이프라인", style="green")
            table.add_column("다음 실행 시간", style="magenta")
            table.add_column("주기(Cron)", style="blue")
            table.add_column("상태", style="yellow")
            
            for schedule in schedules:
                status = "[green]활성화" if schedule.get("enabled", False) else "[red]비활성화"
                table.add_row(
                    schedule["schedule_id"],
                    schedule["pipeline_id"],
                    schedule["next_run"],
                    schedule["cron_expression"],
                    status
                )
                
            console.print(table)
        else:
            console.print("[yellow]⚠ 등록된 스케줄이 없습니다. 스케줄러는 실행되지만 실행할 작업이 없습니다.[/]")
            console.print(f"[blue]스케줄 등록 방법: dteg schedule add <파이프라인_설정_파일> --cron='* * * * *'[/]")
        
        # 스케줄러 시작
        orchestrator.start_scheduler(interval=interval)
        console.print(f"[bold green]✓[/] 스케줄러가 시작되었습니다 (간격: {interval}초)")
        
        if not daemon:
            console.print("[yellow]Ctrl+C로 중지할 수 있습니다...[/]")
            try:
                # 포그라운드 모드에서는 메인 스레드가 계속 실행되어야 함
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                console.print("\n[yellow]스케줄러를 중지합니다...[/]")
                orchestrator.stop_scheduler()
                console.print("[bold green]✓[/] 스케줄러가 중지되었습니다.")
    except Exception as e:
        console.print(f"[bold red]✗[/] 스케줄러 시작 중 오류 발생: {str(e)}")
        sys.exit(1)


@scheduler.command("stop")
def stop_scheduler():
    """스케줄러 중지"""
    try:
        orchestrator = get_orchestrator()
        orchestrator.stop_scheduler()
        console.print("[bold green]✓[/] 스케줄러가 중지되었습니다.")
    except Exception as e:
        console.print(f"[bold red]✗[/] 스케줄러 중지 중 오류 발생: {str(e)}")
        sys.exit(1)


@scheduler.command("status")
def scheduler_status():
    """스케줄러 실행 상태 확인"""
    try:
        orchestrator = get_orchestrator()
        is_running = orchestrator.scheduler_running
        
        if is_running:
            console.print("[bold green]✓[/] 스케줄러가 현재 실행 중입니다.")
        else:
            console.print("[yellow]스케줄러가 현재 중지되어 있습니다.[/]")
    except Exception as e:
        console.print(f"[bold red]✗[/] 스케줄러 상태 확인 중 오류 발생: {str(e)}")
        sys.exit(1)


@scheduler.command("run-once")
@click.option("--verbose", "-v", is_flag=True, help="자세한 로그 출력")
@click.option("--log-level", type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]), default=None, help="로그 레벨 설정")
@click.option("--force", "-f", is_flag=True, help="스케줄 시간과 상관없이 강제 실행")
def scheduler_run_once(verbose: bool, log_level: Optional[str], force: bool):
    """스케줄러를 한 번만 실행하여 대기 중인 스케줄 확인 및 실행"""
    try:
        # 로그 레벨 조정
        import logging
        import sys
        
        # 로깅 셋업
        selected_level = logging.DEBUG if verbose else (getattr(logging, log_level) if log_level else logging.INFO)
        dteg_logger = logging.getLogger('dteg')
        dteg_logger.setLevel(selected_level)
        
        # 핸들러 설정 확인
        has_stdout_handler = False
        for handler in dteg_logger.handlers:
            if isinstance(handler, logging.StreamHandler) and getattr(handler, 'stream', None) == sys.stdout:
                has_stdout_handler = True
                handler.setLevel(selected_level)
                break
                
        # 콘솔 출력 핸들러 추가
        if not has_stdout_handler:
            # 새 핸들러 추가
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(selected_level)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(formatter)
            dteg_logger.addHandler(console_handler)
            
        console.print(f"[yellow]로그 레벨이 {logging.getLevelName(selected_level)}로 설정되었습니다.[/]")
        
        # 출력 버퍼 설정
        sys.stdout.flush()
        
        orchestrator = get_orchestrator()
        
        # 등록된 스케줄 정보 표시
        schedules = orchestrator.get_all_pipelines()
        
        if schedules:
            table = Table(title="등록된 스케줄 목록")
            table.add_column("ID", style="cyan")
            table.add_column("파이프라인", style="green")
            table.add_column("다음 실행 시간", style="magenta")
            table.add_column("주기(Cron)", style="blue")
            table.add_column("상태", style="yellow")
            
            for schedule in schedules:
                status = "[green]활성화" if schedule.get("enabled", False) else "[red]비활성화"
                table.add_row(
                    schedule["schedule_id"],
                    schedule["pipeline_id"],
                    schedule["next_run"],
                    schedule["cron_expression"],
                    status
                )
                
            console.print(table)
        else:
            console.print("[yellow]⚠ 등록된 스케줄이 없습니다.[/]")
            return
            
        # 강제 실행 모드
        if force:
            console.print("[bold yellow]강제 실행 모드가 활성화되었습니다. 모든 활성화된 스케줄을 강제 실행합니다.[/]")
            from datetime import datetime, timedelta
            
            # 모든 스케줄의 다음 실행 시간을 과거로 설정
            for schedule_id in orchestrator.scheduler.schedules:
                schedule = orchestrator.scheduler.get_schedule(schedule_id)
                if schedule and schedule.enabled:
                    # 현재 시간보다 1분 전으로 설정하여 즉시 실행되도록 함
                    schedule.next_run = datetime.now() - timedelta(minutes=1)
        
        # 스케줄러 한 번 실행
        console.print("[bold blue]스케줄 실행 시작...[/]")
        orchestrator.scheduler.run_once()
        sys.stdout.flush()  # 출력 버퍼 강제 플러시
        console.print("[bold green]✓[/] 스케줄 실행이 완료되었습니다.")
        
    except Exception as e:
        console.print(f"[bold red]✗[/] 스케줄 실행 중 오류 발생: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    cli() 
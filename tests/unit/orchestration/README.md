# 오케스트레이션 모듈 테스트

이 디렉토리는 DTEG 오케스트레이션 모듈에 대한 단위 테스트를 포함하고 있습니다.

## 테스트 파일 구조

- `test_scheduler.py`: 스케줄러 모듈 단위 테스트
- `test_worker.py`: Celery 워커 모듈 단위 테스트
- `test_orchestrator.py`: 오케스트레이터 모듈 단위 테스트

## 단위 테스트 실행 방법

단위 테스트는 실제 Redis 서버 없이도 실행 가능합니다. 각 컴포넌트가 독립적으로 올바르게 동작하는지 확인합니다.

```bash
# 모든 오케스트레이션 단위 테스트 실행
python -m unittest discover -s tests/unit/orchestration

# 특정 테스트 파일만 실행
python -m unittest tests/unit/orchestration/test_scheduler.py
python -m unittest tests/unit/orchestration/test_worker.py
python -m unittest tests/unit/orchestration/test_orchestrator.py
```

## 통합 테스트 실행 방법

통합 테스트는 Redis 서버가 실행 중이어야 합니다. 오케스트레이션 컴포넌트들이 통합적으로 잘 작동하는지 확인합니다.

```bash
# Redis 서버 실행
redis-server

# 다른 터미널에서 통합 테스트 실행
python -m unittest tests/integration/test_orchestration.py
```

Redis 서버가 없거나 실행할 수 없는 환경에서는 환경 변수를 설정하여 통합 테스트를 건너뛸 수 있습니다:

```bash
SKIP_INTEGRATION_TESTS=1 python -m unittest tests/integration/test_orchestration.py
```

## 테스트 범위

테스트는 다음과 같은 기능을 검증합니다:

### 스케줄러 테스트
- 스케줄 설정 및 관리
- cron 표현식 기반 스케줄링
- 파이프라인 의존성 확인
- 실행 기록 관리
- 재시도 메커니즘

### 워커 테스트
- Celery 작업 큐 설정
- 비동기 파이프라인 실행
- 태스크 상태 관리
- 작업 취소 기능

### 오케스트레이터 테스트
- 파이프라인 관리 기능
- 스케줄링과 작업 큐 통합
- 파이프라인 의존성 관리
- 스케줄러 스레드 관리

### 통합 테스트
- 오케스트레이션 컴포넌트들의 통합 동작
- 파이프라인 등록부터 실행까지의 전체 흐름
- 의존성 관리 및 스케줄러 동작 
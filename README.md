# DTEG (Data Transfer Engineering Group)

데이터 파이프라인 구축을 위한 확장 가능한 ETL 도구

## 🚀 소개

DTEG은 다양한 데이터 소스에서 데이터를 추출하여 다른 시스템으로 전송하는 과정을 자동화하는 확장 가능한 ETL(Extract, Transform, Load) 도구입니다. 모듈식 아키텍처와 플러그인 시스템을 통해 사용자는 쉽게 자신의 데이터 소스와 대상을 통합할 수 있습니다.

## ✨ 주요 기능

- **모듈식 설계**: 다양한 데이터 소스 및 대상 시스템과의 간편한 통합
- **YAML 기반 설정**: 코드 없이 데이터 파이프라인 정의
- **플러그인 시스템**: 사용자 정의 확장 기능 지원
- **증분 추출**: 변경된 데이터만 효율적으로 처리
- **스케줄링 및 오케스트레이션**: 정기적인 작업 실행 및 종속성 관리
- **로깅 시스템**: 자동 로그 파일 생성 및 세부 로깅 제공

## 🛠️ 설치

```bash
pip install dteg
```

## 📋 사용 예시

```yaml
# pipeline.yaml
version: 1
pipeline:
  name: mysql-to-bigquery
  
  source:
    type: mysql
    config:
      host: localhost
      database: mydb
      user: ${MYSQL_USER}
      password: ${MYSQL_PASSWORD}
      query: SELECT * FROM users WHERE updated_at > '{{ last_run }}'
  
  destination:
    type: bigquery
    config:
      project: my-project
      dataset: mydataset
      table: users
      if_exists: append
```

## 🌱 개발 중인 프로젝트

DTEG은 현재 개발 중인 프로젝트로, 안정적인 버전은 아직 출시되지 않았습니다. 개발에 참여하고 싶다면 아래 Contributing 섹션을 참고해주세요.

## 📟 CLI 사용 가이드

DTEG의 CLI 도구를 통해 데이터 파이프라인을 구성하고 실행할 수 있습니다. 아래는 주요 명령어와 사용법입니다.

### 기본 명령어

```bash
# 도움말 확인
dteg --help

# 버전 확인
dteg --version
```

### 프로젝트 초기화

```bash
# 현재 디렉토리에 새 프로젝트 초기화
dteg init

# 지정한 디렉토리에 새 프로젝트 초기화
dteg init my_project
```

### 파이프라인 실행

```bash
# 파이프라인 실행
dteg run pipeline_config.yaml

# 상세 로그와 함께 실행
dteg run pipeline_config.yaml --verbose

# 로그 파일 지정하여 실행
dteg run pipeline_config.yaml --log-file=my_pipeline.log

# 설정만 검증
dteg run pipeline_config.yaml --validate-only
```

### 파이프라인 설정 검증

```bash
# 파이프라인 설정 파일 검증
dteg validate pipeline_config.yaml
```

### 시스템 정보 확인

```bash
# DTEG 및 시스템 정보 표시
dteg info
```

### 스케줄 관리

```bash
# 스케줄 추가
dteg schedule add pipeline_config.yaml --cron="*/5 * * * *"

# 스케줄 목록 조회
dteg schedule list

# 스케줄 업데이트
dteg schedule update <schedule_id> --cron="0 */1 * * *" --enabled

# 스케줄 삭제
dteg schedule delete <schedule_id>

# 스케줄 즉시 실행
dteg schedule run <schedule_id>
```

### 스케줄러 제어

```bash
# 스케줄러 시작 (자동으로 로그 파일 생성됨)
dteg scheduler start

# 인터벌 설정하여 시작 (기본값: 60초)
dteg scheduler start --interval=30

# 기본적으로 즉시 실행됨 (no_immediate_run=False가 기본값)
# 즉시 실행 없이 시작하려면 --no-immediate-run 옵션 사용
dteg scheduler start --no-immediate-run

# 로그 레벨과 로그 파일 지정하여 시작
dteg scheduler start --verbose --log-file=my_scheduler.log

# 스케줄러 상태 확인
dteg scheduler status

# 스케줄러 중지
dteg scheduler stop

# 스케줄러 한 번만 실행 (자동으로 로그 파일 생성됨)
dteg scheduler run-once

# 강제 실행 모드로 한 번 실행 (예약 시간 무시)
dteg scheduler run-once --force --log-file=forced_run.log
```

### 로깅 시스템

DTEG는 자동 로그 파일 생성 기능을 제공합니다:

```bash
# 기본 로그 파일은 logs/ 디렉토리에 자동 생성됨
# 형식: scheduler_YYYYMMDD-HHMMSS.log 또는 scheduler_run_once_YYYYMMDD-HHMMSS.log

# 로그 파일 경로 직접 지정
dteg scheduler start --log-file=custom_path.log

# 로그 레벨 설정
dteg scheduler start --log-level=DEBUG
dteg scheduler run-once --verbose  # verbose는 DEBUG 레벨과 동일
```

### 파이프라인 설정 예시

파이프라인 설정 파일(`pipeline_config.yaml`) 예시:

```yaml
pipeline_id: my_pipeline
name: 샘플 파이프라인
description: 데이터 추출 및 변환 파이프라인

schedule:
  cron: "*/5 * * * *"  # 5분마다 실행 (cron 표현식)
  enabled: true        # 스케줄 활성화 여부

extractors:
  # 추출기 설정...
transformers:
  # 변환기 설정...
loaders:
  # 로더 설정...
```

### 주의사항

- 스케줄러 사용 시, 구성한 파이프라인의 설정 파일에 `pipeline_id`와 `schedule` 섹션이 올바르게 설정되어 있어야 합니다.
- 스케줄 목록을 확인하려면 `dteg schedule list` 명령을 사용하세요.
- `dteg scheduler run-once` 명령을 사용하면 스케줄러가 한 번만 실행되어 대기 중인 모든 파이프라인을 처리합니다.
- 기본적으로 스케줄러 시작 시 즉시 실행됩니다(no_immediate_run=False가 기본값). 이를 방지하려면 `--no-immediate-run` 옵션을 사용하세요.
- 모든 스케줄러 명령은 자동으로 로그 파일을 생성하며, 실행 시 로그 파일 경로가 콘솔에 표시됩니다.
- 로그 파일은 기본적으로 현재 작업 디렉토리의 `logs/` 폴더에 저장됩니다.

## 🌐 웹 UI 사용 가이드

DTEG는 웹 기반 사용자 인터페이스를 제공합니다. 웹 UI를 통해 파이프라인을 관리하고 모니터링할 수 있습니다.

### 웹 서버 시작

```bash
# 기본 설정으로 웹 서버 시작 (기본 포트: 8000)
dteg web start

# 포트 변경하여 시작
dteg web start --port=8080

# 개발 모드로 시작 (코드 변경 시 자동 재시작)
dteg web start --reload
```

### 웹 UI 접속

웹 서버가 시작되면 브라우저에서 다음 URL로 접속할 수 있습니다:
```
http://localhost:8000
```

### 관리자 계정 설정

웹 UI에 로그인하기 위한 기본 관리자 계정 정보:
- 사용자명: admin
- 비밀번호: admin

보안을 위해 실제 운영 환경에서는 관리자 계정 정보를 변경하는 것을 강력히 권장합니다. 
다음 환경 변수를 설정하여 기본 관리자 계정 정보를 변경할 수 있습니다:

```bash
# Linux/Mac에서 환경 변수 설정
export DTEG_ADMIN_USERNAME="my_admin"
export DTEG_ADMIN_PASSWORD="secure_password"
export DTEG_ADMIN_EMAIL="admin@mycompany.com"
export DTEG_ADMIN_FULLNAME="시스템 관리자"

# Windows에서 환경 변수 설정
set DTEG_ADMIN_USERNAME=my_admin
set DTEG_ADMIN_PASSWORD=secure_password
set DTEG_ADMIN_EMAIL=admin@mycompany.com
set DTEG_ADMIN_FULLNAME=시스템 관리자

# Docker 환경에서 설정 (docker-compose.yml 예시)
version: '3'
services:
  dteg:
    image: dteg/dteg-core
    environment:
      - DTEG_ADMIN_USERNAME=my_admin
      - DTEG_ADMIN_PASSWORD=secure_password
      - DTEG_ADMIN_EMAIL=admin@mycompany.com
      - DTEG_ADMIN_FULLNAME=시스템 관리자
```

이 환경 변수들은 시스템이 처음 시작될 때만 사용됩니다. 이미 관리자 계정이 생성된 후에는 웹 UI의 사용자 관리 기능을 통해 계정 정보를 변경할 수 있습니다.

## 🤝 기여하기

기여는 언제나 환영합니다! 자세한 내용은 [CONTRIBUTING.md](CONTRIBUTING.md)를 참조하세요.

## 📄 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다. 자세한 내용은 [LICENSE](LICENSE) 파일을 참조하세요.

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
# 스케줄러 시작
dteg scheduler start

# 인터벌 설정하여 시작 (기본값: 60초)
dteg scheduler start --interval=30

# 스케줄러 상태 확인
dteg scheduler status

# 스케줄러 중지
dteg scheduler stop

# 스케줄러 한 번만 실행
dteg scheduler run-once

# 강제 실행 모드로 한 번 실행 (예약 시간 무시)
dteg scheduler run-once --force
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

## 🤝 기여하기

기여는 언제나 환영합니다! 자세한 내용은 [CONTRIBUTING.md](CONTRIBUTING.md)를 참조하세요.

## 📄 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다. 자세한 내용은 [LICENSE](LICENSE) 파일을 참조하세요.

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

## 🤝 기여하기

기여는 언제나 환영합니다! 자세한 내용은 [CONTRIBUTING.md](CONTRIBUTING.md)를 참조하세요.

## 📄 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다. 자세한 내용은 [LICENSE](LICENSE) 파일을 참조하세요.

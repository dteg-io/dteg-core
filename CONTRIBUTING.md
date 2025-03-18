# 기여 가이드라인

DTEG 프로젝트에 기여해 주셔서 감사합니다! 이 문서는 개발 환경 설정부터 풀 리퀘스트 제출까지의 과정을 안내합니다.

## 개발 환경 설정

1. 저장소 클론:
   ```bash
   git clone https://github.com/dteg-io/dteg-core.git
   cd dteg-core
   ```

2. 가상 환경 생성 및 활성화:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Windows의 경우: venv\Scripts\activate
   ```

3. 개발 의존성 설치:
   ```bash
   pip install -e ".[dev]"
   ```

4. pre-commit 훅 설치:
   ```bash
   pre-commit install
   ```

## 코드 스타일

이 프로젝트는 다음 도구를 사용하여 코드 스타일과 품질을 유지합니다:

- **Black**: 자동 코드 형식 지정
- **isort**: 임포트 정렬
- **flake8**: 린팅
- **mypy**: 정적 타입 검사

커밋하기 전에 코드 스타일을 확인하려면:

```bash
# 자동 형식 지정
black src tests
isort src tests

# 린팅 및 타입 검사
flake8 src tests
mypy src tests
```

## 테스트 실행

테스트를 실행하기 위해:

```bash
pytest
```

커버리지 보고서 생성:

```bash
pytest --cov=src/dteg
```

## 브랜치 전략

기능 개발 시:

1. `develop` 브랜치에서 새 브랜치 생성:
   ```bash
   git checkout -b feature/기능-이름
   ```

2. 작업 완료 후 `develop` 브랜치로 PR 생성

버그 수정 시:

1. `develop` 브랜치에서 새 브랜치 생성:
   ```bash
   git checkout -b fix/버그-설명
   ```

2. 작업 완료 후 `develop` 브랜치로 PR 생성

## 커밋 메시지 규칙

커밋 메시지는 다음 형식을 따릅니다:

```
<타입>: <변경 내용>
```

타입 목록:
- **feat**: 새 기능 추가
- **fix**: 버그 수정
- **docs**: 문서 변경
- **style**: 코드 형식 변경 (기능 변경 없음)
- **refactor**: 코드 리팩토링
- **test**: 테스트 추가 또는 수정
- **chore**: 빌드 프로세스 또는 도구 변경

예:
```
feat: MySQL Extractor 구현
fix: 설정 파일 파싱 오류 수정
docs: README에 사용 예시 추가
```

## 풀 리퀘스트 가이드라인

1. PR 설명을 자세히 작성해주세요
2. 관련 이슈가 있다면 '#이슈번호'로 연결해주세요
3. 모든 테스트가 통과하는지 확인해주세요
4. PR이 한 가지 변경사항만을 다루는지 확인해주세요

감사합니다!

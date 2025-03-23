"""
DTEG Web API - 메인 모듈

FastAPI 애플리케이션 및 API 라우터 설정
"""
from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.responses import JSONResponse, FileResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import os
from pathlib import Path

from dteg.web.database import get_db, init_db
from dteg.web.api import executions, pipelines, schedules, auth
from dteg.orchestration import get_orchestrator  # 추가: 오케스트레이터 가져오기

# FastAPI 애플리케이션 생성
app = FastAPI(
    title="DTEG Web API",
    description="데이터 파이프라인 관리 웹 API",
    version="0.1.0"
)

# CORS 미들웨어 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 시작 시 데이터베이스 초기화
@app.on_event("startup")
async def startup_db_client():
    init_db()
    
    # 추가: 웹 서버 시작 시 스케줄 동기화
    try:
        orchestrator = get_orchestrator()
        orchestrator.sync_schedules_with_web_db()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"웹 서버 시작 시 스케줄 동기화 실패: {str(e)}")

# 정적 파일 디렉토리 경로
static_dir = Path(__file__).parent.parent / "static"

# 정적 파일 마운트 - URL 경로를 수정하여 직접 루트에서 접근 가능하게 함
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# CSS 파일 제공
@app.get("/css/{file_path:path}")
async def get_css(file_path: str):
    return FileResponse(static_dir / "css" / file_path)

# JS 파일 제공
@app.get("/js/{file_path:path}")
async def get_js(file_path: str):
    return FileResponse(static_dir / "js" / file_path)

# favicon 제공 - 오류 수정
@app.get("/favicon.ico")
async def get_favicon():
    favicon_path = static_dir / "favicon.ico"
    if favicon_path.exists():
        with open(favicon_path, "rb") as f:
            content = f.read()
        return Response(content=content, media_type="image/x-icon")
    # favicon이 없으면 빈 응답 반환
    return Response(content=b"", media_type="image/x-icon")

# 웹 인터페이스 라우팅 - 정적 파일로 제공
@app.get("/")
async def get_index():
    index_path = static_dir / "index.html"
    return FileResponse(index_path)

# API 라우터 등록
app.include_router(auth.router, prefix="/api/auth", tags=["인증"])
app.include_router(executions.router, prefix="/api/executions", tags=["실행"])
app.include_router(pipelines.router, prefix="/api/pipelines", tags=["파이프라인"])
app.include_router(schedules.router, prefix="/api/schedules", tags=["스케줄"])

# 기본 라우터
@app.get("/api/health", tags=["시스템"])
async def health_check():
    """API 서버 상태 확인"""
    return {"status": "healthy", "version": app.version}

@app.get("/api/config", tags=["시스템"])
async def get_config():
    """시스템 설정 정보 조회"""
    return {
        "version": app.version,
        "environment": os.environ.get("DTEG_ENV", "development"),
    }

# 앱 실행 (개발 모드)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("dteg.web.api.main:app", host="0.0.0.0", port=8000, reload=True) 
"""
DTEG Web API Main App

FastAPI 애플리케이션 진입점
"""
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pathlib import Path
import os

from dteg.core.config import Config
from dteg.orchestration.orchestrator import Orchestrator
from dteg.orchestration import get_orchestrator
from dteg.web.api import models
from dteg.web.api.auth import router as auth_router
from dteg.web.api.pipelines import router as pipelines_router
from dteg.web.api.schedules import router as schedules_router
from dteg.web.api.executions import router as executions_router

# FastAPI 앱 생성
app = FastAPI(
    title="DTEG API",
    description="Data Transfer Engineering Group API",
    version="0.1.0",
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 프로덕션에서는 실제 도메인으로 제한해야 함
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 라우터 등록
app.include_router(auth_router, prefix="/api/auth", tags=["인증"])
app.include_router(pipelines_router, prefix="/api/pipelines", tags=["파이프라인"])
app.include_router(schedules_router, prefix="/api/schedules", tags=["스케줄"])
app.include_router(executions_router, prefix="/api/executions", tags=["실행 이력"])

# 정적 파일 마운트 (프론트엔드 빌드 파일)
static_dir = Path(__file__).parent.parent / "static"
if static_dir.exists():
    app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")

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
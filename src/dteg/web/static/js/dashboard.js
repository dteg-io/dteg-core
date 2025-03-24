/**
 * DTEG Web UI - 대시보드 관련 기능
 */

// 대시보드 로드 함수
async function loadDashboard() {
    try {
        // 파이프라인 수 로드
        const pipelines = await DtegApi.getPipelines();
        pageHandlers.dashboard.elements.totalPipelines.textContent = pipelines.length || 0;

        // 스케줄 수 로드
        const schedules = await DtegApi.getSchedules();
        const activeSchedules = schedules.filter(s => s.enabled).length || 0;
        pageHandlers.dashboard.elements.activeSchedules.textContent = activeSchedules;

        // 전체 실행 통계 로드
        const metrics = await DtegApi.getDashboardMetrics();
        console.log('메트릭 응답:', metrics); // 디버깅을 위해 로그 추가

        // 완료된 실행과 실패한 실행 수 표시 (전체 통계 사용)
        pageHandlers.dashboard.elements.completedExecutions.textContent =
            metrics.pipeline_status.completed || 0;
        pageHandlers.dashboard.elements.failedExecutions.textContent =
            metrics.pipeline_status.failed || 0;

        // 최근 실행 이력만 별도로 로드 (테이블용)
        const recentExecutions = await DtegApi.getRecentExecutions(5);
        console.log('최근 실행 이력 응답:', recentExecutions); // 디버깅을 위해 로그 추가

        // 최근 실행 이력 테이블 채우기
        renderExecutionsList(pageHandlers.dashboard.elements.recentExecutionsTbody, recentExecutions);
    } catch (error) {
        console.error('대시보드 로드 오류:', error);
        showToast('대시보드 정보를 불러오는 중 오류가 발생했습니다', 'error');
    }
} 
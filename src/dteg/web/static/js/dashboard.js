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

        // 직접 실행 이력을 모두 가져와 카운트 검증
        console.log('실행 이력 카운트 검증 시작');
        const executionsRaw = await DtegApi.fetchApi('/executions?page=1&page_size=100');
        const executions = Array.isArray(executionsRaw) ? executionsRaw :
            (executionsRaw.executions && Array.isArray(executionsRaw.executions)) ?
                executionsRaw.executions : [];

        // 실제 완료된 실행 수 계산
        const completedCount = executions.filter(e => e.status && e.status.toLowerCase() === 'completed').length;
        console.log('실제 완료된 실행 수 (직접 카운트):', completedCount);
        console.log('API에서 제공하는 완료된 실행 수:', metrics.pipeline_status.completed);

        // 완료된 실행과 실패한 실행 수 표시 (전체 통계 사용)
        const apiCompletedCount = metrics.pipeline_status.completed || 0;

        // 데이터 일치 여부 표시
        const displayCount = apiCompletedCount === completedCount ?
            apiCompletedCount.toString() :
            `${completedCount} (로컬 파일 기준)`;

        pageHandlers.dashboard.elements.completedExecutions.textContent = displayCount;

        // 실패한 실행 수 계산 및 표시
        const failedCount = executions.filter(e => e.status && e.status.toLowerCase() === 'failed').length;
        const apiFailed = metrics.pipeline_status.failed || 0;

        pageHandlers.dashboard.elements.failedExecutions.textContent =
            apiFailed === failedCount ?
                apiFailed.toString() :
                `${failedCount} (로컬 파일 기준)`;

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
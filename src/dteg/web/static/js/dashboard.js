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

        // 실행 이력 로드
        const executions = await DtegApi.getExecutions(1, 5);
        console.log('Executions 응답:', executions); // 디버깅을 위해 로그 추가

        // executions가 배열인 경우(페이지네이션 객체가 아닌 경우)
        let executionsArray = Array.isArray(executions) ? executions : (executions.executions || []);

        const completed = executionsArray.filter(e => e.status.toLowerCase() === 'completed').length || 0;
        const failed = executionsArray.filter(e => e.status.toLowerCase() === 'failed').length || 0;

        pageHandlers.dashboard.elements.completedExecutions.textContent = completed;
        pageHandlers.dashboard.elements.failedExecutions.textContent = failed;

        // 최근 실행 이력 테이블 채우기
        renderExecutionsList(pageHandlers.dashboard.elements.recentExecutionsTbody, executionsArray);
    } catch (error) {
        console.error('대시보드 로드 오류:', error);
        showToast('대시보드 정보를 불러오는 중 오류가 발생했습니다', 'error');
    }
} 
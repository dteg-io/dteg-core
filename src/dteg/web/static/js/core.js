/**
 * DTEG Web UI - 핵심 유틸리티 및 공통 기능
 */

// DOM 요소
const navLinks = document.querySelectorAll('.nav-link[data-page]');
const contentPages = document.querySelectorAll('.content-page');
const loginModal = new bootstrap.Modal(document.getElementById('loginModal'));
const loginForm = document.getElementById('login-form');
const logoutBtn = document.getElementById('logout-btn');
const usernameSpan = document.getElementById('username');

// 페이지별 요소 (핸들러는 나중에 할당)
const pageHandlers = {
    dashboard: {
        elements: {
            totalPipelines: document.getElementById('total-pipelines'),
            activeSchedules: document.getElementById('active-schedules'),
            completedExecutions: document.getElementById('completed-executions'),
            failedExecutions: document.getElementById('failed-executions'),
            recentExecutionsTbody: document.getElementById('recent-executions-tbody')
        }
    },
    pipelines: {
        elements: {
            createBtn: document.getElementById('create-pipeline-btn'),
            tbody: document.getElementById('pipelines-tbody')
        }
    },
    schedules: {
        elements: {
            createBtn: document.getElementById('create-schedule-btn'),
            tbody: document.getElementById('schedules-tbody')
        }
    },
    executions: {
        elements: {
            tbody: document.getElementById('executions-tbody'),
            pagination: document.getElementById('executions-pagination')
        }
    }
};

// 유틸리티 함수
function showToast(message, type = 'info') {
    // 간단한 알림 메시지 표시 (향후 실제 토스트 구현)
    alert(message);
}

function showLoginModal() {
    loginModal.show();
}

function formatDate(dateStr) {
    if (!dateStr) return '없음';
    const date = new Date(dateStr);
    return date.toLocaleString('ko-KR');
}

function getStatusBadge(status) {
    const statusMap = {
        'completed': '<span class="badge bg-success">완료</span>',
        'running': '<span class="badge bg-primary">실행 중</span>',
        'failed': '<span class="badge bg-danger">실패</span>',
        'pending': '<span class="badge bg-warning">대기 중</span>',
        'canceled': '<span class="badge bg-secondary">취소됨</span>'
    };

    return statusMap[status.toLowerCase()] || `<span class="badge bg-info">${status}</span>`;
}

function clearTable(tbody) {
    tbody.innerHTML = '<tr><td colspan="6" class="text-center">데이터를 로드 중입니다...</td></tr>';
}

// 페이지 전환 처리
function showPage(pageId) {
    // 네비게이션 링크 상태 변경
    navLinks.forEach(link => {
        if (link.dataset.page === pageId) {
            link.classList.add('active');
        } else {
            link.classList.remove('active');
        }
    });

    // 페이지 표시 전환
    contentPages.forEach(page => {
        if (page.id === `${pageId}-page`) {
            page.classList.remove('d-none');

            // 페이지 초기화 함수 호출
            if (pageHandlers[pageId] && pageHandlers[pageId].init) {
                pageHandlers[pageId].init();
            }
        } else {
            page.classList.add('d-none');
        }
    });
}

// 모든 스크립트가 로드된 후 초기화
document.addEventListener('DOMContentLoaded', () => {
    // 페이지 핸들러 초기화 함수 할당
    pageHandlers.dashboard.init = loadDashboard;
    pageHandlers.pipelines.init = loadPipelines;
    pageHandlers.schedules.init = loadSchedules;
    pageHandlers.executions.init = loadExecutions;

    // 페이지 네비게이션 이벤트
    navLinks.forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            showPage(link.dataset.page);
        });
    });

    // 로그인 폼 제출 이벤트
    loginForm.addEventListener('submit', handleLogin);

    // 로그아웃 버튼 클릭 이벤트
    logoutBtn.addEventListener('click', (e) => {
        e.preventDefault();
        handleLogout();
    });

    // 파이프라인 생성 버튼 이벤트
    pageHandlers.pipelines.elements.createBtn.addEventListener('click', () => {
        showCreatePipelineModal();
    });

    // 스케줄 생성 버튼 이벤트
    pageHandlers.schedules.elements.createBtn.addEventListener('click', () => {
        showCreateScheduleModal();
    });

    // 인증 상태 확인
    if (!DtegApi.isAuthenticated()) {
        showLoginModal();
    } else {
        // 사용자 정보 로드 시도
        DtegApi.getCurrentUser().then(user => {
            usernameSpan.textContent = user.username;
        }).catch(() => {
            // 인증 토큰이 유효하지 않은 경우
            DtegApi.clearToken();
            showLoginModal();
        });
    }

    // 기본 페이지 로드
    showPage('dashboard');

    // 모달 버튼에 대한 이벤트 리스너 설정 (모달이 동적으로 추가되는 경우)
    document.body.addEventListener('click', function (e) {
        if (e.target && e.target.id === 'save-schedule-btn') {
            createSchedule();
        }
        if (e.target && e.target.id === 'save-pipeline-btn') {
            createPipeline();
        }
    });
}); 
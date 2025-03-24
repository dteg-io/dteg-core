/**
 * DTEG Web UI - 실행 이력 관련 기능
 */

// 실행 이력 로드
async function loadExecutions(page = 1) {
    const tbody = pageHandlers.executions.elements.tbody;
    const pagination = pageHandlers.executions.elements.pagination;

    clearTable(tbody);

    try {
        const executions = await DtegApi.getExecutions(page, 10);
        console.log('실행 이력 로드 응답:', executions); // 디버깅을 위해 로그 추가

        // executions가 배열인 경우 처리
        let executionsArray = Array.isArray(executions) ? executions : (executions.executions || []);
        let total = Array.isArray(executions) ? executionsArray.length : (executions.total || executionsArray.length);
        let currentPage = Array.isArray(executions) ? 1 : (executions.page || 1);
        let pageSize = Array.isArray(executions) ? 10 : (executions.page_size || 10);

        if (executionsArray.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" class="text-center">실행 이력이 없습니다</td></tr>';
            pagination.classList.add('d-none');
            return;
        }

        renderExecutionsList(tbody, executionsArray);
        renderPagination(pagination, total, currentPage, pageSize);
    } catch (error) {
        console.error('실행 이력 로드 오류:', error);
        tbody.innerHTML = '<tr><td colspan="6" class="text-center text-danger">실행 이력을 불러오는 중 오류가 발생했습니다</td></tr>';
    }
}

// 실행 이력 리스트 렌더링
function renderExecutionsList(tbody, executions) {
    tbody.innerHTML = executions.map(execution => `
        <tr>
            <td>${execution.id.substring(0, 8)}...</td>
            <td>${execution.pipeline_id.substring(0, 8)}...</td>
            <td>${getStatusBadge(execution.status)}</td>
            <td>${formatDate(execution.started_at)}</td>
            <td>${formatDate(execution.ended_at)}</td>
            <td>
                <button class="btn btn-sm btn-info view-logs" data-id="${execution.id}">로그</button>
                <button class="btn btn-sm btn-danger delete-execution" data-id="${execution.id}">삭제</button>
            </td>
        </tr>
    `).join('');

    // 이벤트 바인딩
    tbody.querySelectorAll('.view-logs').forEach(btn => {
        btn.addEventListener('click', () => viewExecutionLogs(btn.dataset.id));
    });

    tbody.querySelectorAll('.delete-execution').forEach(btn => {
        btn.addEventListener('click', () => deleteExecution(btn.dataset.id));
    });
}

// 페이지네이션 렌더링
function renderPagination(paginationEl, total, currentPage, pageSize) {
    if (total <= pageSize) {
        paginationEl.classList.add('d-none');
        return;
    }

    paginationEl.classList.remove('d-none');

    const totalPages = Math.ceil(total / pageSize);
    let paginationHtml = '';

    // 이전 버튼
    paginationHtml += `
        <li class="page-item ${currentPage <= 1 ? 'disabled' : ''}">
            <a class="page-link" href="#" data-page="${currentPage - 1}">이전</a>
        </li>
    `;

    // 페이지 번호 표시 로직 개선
    // 항상 첫 페이지, 마지막 페이지 표시하고, 현재 페이지 주변 일부만 표시

    // 표시할 페이지 범위 설정
    const showFirstPage = true;
    const showLastPage = true;
    const pagesAroundCurrent = 1; // 현재 페이지 양쪽에 표시할 페이지 수

    let pagesToShow = [];

    // 현재 페이지 주변 페이지 추가
    for (let i = Math.max(1, currentPage - pagesAroundCurrent);
        i <= Math.min(totalPages, currentPage + pagesAroundCurrent); i++) {
        pagesToShow.push(i);
    }

    // 첫 페이지 추가 (아직 없는 경우)
    if (showFirstPage && !pagesToShow.includes(1)) {
        pagesToShow.unshift(1);

        // 첫 페이지와 표시된 가장 작은 페이지 사이에 갭이 있으면 생략 표시 추가
        if (pagesToShow[1] > 2) {
            pagesToShow.splice(1, 0, 'ellipsis-start');
        }
    }

    // 마지막 페이지 추가 (아직 없는 경우)
    if (showLastPage && !pagesToShow.includes(totalPages) && totalPages > 1) {
        // 표시된 가장 큰 페이지와 마지막 페이지 사이에 갭이 있으면 생략 표시 추가
        if (pagesToShow[pagesToShow.length - 1] < totalPages - 1) {
            pagesToShow.push('ellipsis-end');
        }
        pagesToShow.push(totalPages);
    }

    // 페이지 버튼 생성
    pagesToShow.forEach(page => {
        if (page === 'ellipsis-start' || page === 'ellipsis-end') {
            paginationHtml += `
                <li class="page-item disabled">
                    <span class="page-link">...</span>
                </li>
            `;
        } else {
            paginationHtml += `
                <li class="page-item ${page === currentPage ? 'active' : ''}">
                    <a class="page-link" href="#" data-page="${page}">${page}</a>
                </li>
            `;
        }
    });

    // 다음 버튼
    paginationHtml += `
        <li class="page-item ${currentPage >= totalPages ? 'disabled' : ''}">
            <a class="page-link" href="#" data-page="${currentPage + 1}">다음</a>
        </li>
    `;

    paginationEl.innerHTML = paginationHtml;

    // 페이지 클릭 이벤트 바인딩
    paginationEl.querySelectorAll('.page-link').forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            if (!link.parentElement.classList.contains('disabled') && link.dataset.page) {
                loadExecutions(parseInt(link.dataset.page));
            }
        });
    });
}

// 실행 로그 보기
async function viewExecutionLogs(executionId) {
    try {
        const logs = await DtegApi.getExecutionLogs(executionId);
        // 구현 예정 - 모달에 로그 표시
        alert(`실행 ID ${executionId}의 로그:\n\n${logs.substring(0, 500)}...`);
    } catch (error) {
        showToast('로그 조회 중 오류가 발생했습니다', 'error');
    }
}

// 실행 기록 삭제
async function deleteExecution(executionId) {
    if (!confirm('정말로 이 실행 이력을 삭제하시겠습니까?')) {
        return;
    }

    try {
        await DtegApi.deleteExecution(executionId);
        showToast('실행 이력이 삭제되었습니다', 'success');
        loadExecutions();
    } catch (error) {
        showToast('실행 이력 삭제 중 오류가 발생했습니다', 'error');
    }
} 
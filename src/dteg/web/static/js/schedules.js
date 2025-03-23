/**
 * DTEG Web UI - 스케줄 관련 기능
 */

// 스케줄 목록 로드
async function loadSchedules() {
    const tbody = pageHandlers.schedules.elements.tbody;
    clearTable(tbody);

    try {
        const schedules = await DtegApi.getSchedules();

        if (schedules.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" class="text-center">스케줄이 없습니다</td></tr>';
            return;
        }

        tbody.innerHTML = schedules.map(schedule => `
            <tr>
                <td>${schedule.id.substring(0, 8)}...</td>
                <td>${schedule.pipeline_id.substring(0, 8)}...</td>
                <td><code>${schedule.cron_expression}</code></td>
                <td>${schedule.enabled ?
                '<span class="badge bg-success">활성</span>' :
                '<span class="badge bg-secondary">비활성</span>'}</td>
                <td>${formatDate(schedule.next_run)}</td>
                <td>
                    <button class="btn btn-sm btn-primary run-schedule" data-id="${schedule.id}">실행</button>
                    <button class="btn btn-sm btn-info edit-schedule" data-id="${schedule.id}">편집</button>
                    <button class="btn btn-sm btn-danger delete-schedule" data-id="${schedule.id}">삭제</button>
                </td>
            </tr>
        `).join('');

        // 버튼 이벤트 바인딩
        tbody.querySelectorAll('.run-schedule').forEach(btn => {
            btn.addEventListener('click', () => runSchedule(btn.dataset.id));
        });

        tbody.querySelectorAll('.edit-schedule').forEach(btn => {
            btn.addEventListener('click', () => editSchedule(btn.dataset.id));
        });

        tbody.querySelectorAll('.delete-schedule').forEach(btn => {
            btn.addEventListener('click', () => deleteSchedule(btn.dataset.id));
        });
    } catch (error) {
        console.error('스케줄 로드 오류:', error);
        tbody.innerHTML = '<tr><td colspan="6" class="text-center text-danger">스케줄을 불러오는 중 오류가 발생했습니다</td></tr>';
    }
}

// 스케줄 실행
async function runSchedule(scheduleId) {
    try {
        await DtegApi.runSchedule(scheduleId);
        showToast('스케줄 실행이 시작되었습니다', 'success');
    } catch (error) {
        showToast('스케줄 실행 중 오류가 발생했습니다', 'error');
    }
}

// 스케줄 편집
function editSchedule(scheduleId) {
    // 구현 예정
    alert(`스케줄 ID ${scheduleId} 편집 기능은 구현 예정입니다.`);
}

// 스케줄 삭제
async function deleteSchedule(scheduleId) {
    if (!confirm('정말로 이 스케줄을 삭제하시겠습니까?')) {
        return;
    }

    try {
        await DtegApi.deleteSchedule(scheduleId);
        showToast('스케줄이 삭제되었습니다', 'success');
        loadSchedules();
    } catch (error) {
        showToast('스케줄 삭제 중 오류가 발생했습니다', 'error');
    }
}

// 스케줄 생성 모달 표시
function showCreateScheduleModal() {
    // 기존에 모달이 있으면 제거
    const existingModal = document.getElementById('schedule-modal');
    if (existingModal) {
        existingModal.remove();
    }

    // 먼저 파이프라인 목록을 가져옴
    DtegApi.getPipelines().then(pipelines => {
        // 파이프라인 목록 옵션 생성
        const pipelineOptions = pipelines.map(p =>
            `<option value="${p.id}">${p.name}</option>`
        ).join('');

        // 모달 HTML 생성
        const modalHtml = `
        <div class="modal fade" id="schedule-modal" tabindex="-1">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title">새 스케줄 생성</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <div class="modal-body">
                        <form id="schedule-form">
                            <div class="mb-3">
                                <label for="schedule-name" class="form-label">스케줄 이름</label>
                                <input type="text" class="form-control" id="schedule-name" required>
                            </div>
                            <div class="mb-3">
                                <label for="pipeline-id" class="form-label">파이프라인</label>
                                <select class="form-select" id="pipeline-id" required>
                                    <option value="">파이프라인 선택</option>
                                    ${pipelineOptions}
                                </select>
                            </div>
                            <div class="mb-3">
                                <label for="cron-expression" class="form-label">Cron 표현식</label>
                                <div class="input-group">
                                    <input type="text" class="form-control" id="cron-expression" 
                                        placeholder="0 0 * * *" required>
                                    <button class="btn btn-outline-secondary" type="button" id="cron-help-btn"
                                        title="Cron 표현식 도움말">?</button>
                                </div>
                                <div class="form-text">예시: 0 0 * * * (매일 자정), 0 */6 * * * (6시간마다)</div>
                            </div>
                            <div class="mb-3 form-check">
                                <input type="checkbox" class="form-check-input" id="enabled" checked>
                                <label class="form-check-label" for="enabled">활성화</label>
                            </div>
                            <div class="mb-3">
                                <label for="parameters" class="form-label">파라미터 (JSON)</label>
                                <textarea class="form-control" id="parameters" rows="3" 
                                    placeholder='{"param1": "value1"}'></textarea>
                            </div>
                        </form>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">취소</button>
                        <button type="button" class="btn btn-primary" id="save-schedule-btn">저장</button>
                    </div>
                </div>
            </div>
        </div>
        `;

        // 모달을 DOM에 추가
        document.body.insertAdjacentHTML('beforeend', modalHtml);

        // 모달 인스턴스 생성 및 표시
        const scheduleModal = new bootstrap.Modal(document.getElementById('schedule-modal'));
        scheduleModal.show();

        // Cron 표현식 도움말 버튼 이벤트
        document.getElementById('cron-help-btn').addEventListener('click', () => {
            alert(`
Cron 표현식 도움말:

* * * * * 
│ │ │ │ │
│ │ │ │ └─ 요일 (0-6, 0=일요일)
│ │ │ └─── 월 (1-12)
│ │ └───── 일 (1-31)
│ └─────── 시 (0-23)
└───────── 분 (0-59)

예시:
0 0 * * *     - 매일 자정
0 */6 * * *   - 6시간마다
0 9 * * 1-5   - 평일 오전 9시
0 0 1 * *     - 매월 1일 자정
`);
        });
    });
}

// 스케줄 생성 처리
async function createSchedule() {
    try {
        // 폼 데이터 수집
        const name = document.getElementById('schedule-name').value;
        const pipelineId = document.getElementById('pipeline-id').value;
        const cronExpression = document.getElementById('cron-expression').value;
        const enabled = document.getElementById('enabled').checked;

        // 유효성 검사
        if (!name) {
            showToast('스케줄 이름을 입력해주세요.', 'error');
            return;
        }

        if (!pipelineId) {
            showToast('파이프라인을 선택해주세요.', 'error');
            return;
        }

        if (!cronExpression) {
            showToast('Cron 표현식을 입력해주세요.', 'error');
            return;
        }

        // 파라미터 JSON 파싱 (입력이 있는 경우)
        let parameters = null;
        const parametersText = document.getElementById('parameters').value.trim();
        if (parametersText) {
            try {
                parameters = JSON.parse(parametersText);
            } catch (e) {
                showToast('파라미터 JSON 형식이 올바르지 않습니다.', 'error');
                return;
            }
        }

        // 스케줄 데이터 생성
        const scheduleData = {
            name,
            pipeline_id: pipelineId,
            cron_expression: cronExpression,
            enabled,
            parameters
        };

        // API 호출
        await DtegApi.createSchedule(scheduleData);

        // 모달 닫기
        const scheduleModal = bootstrap.Modal.getInstance(document.getElementById('schedule-modal'));
        scheduleModal.hide();

        // 성공 메시지 표시
        showToast('스케줄이 성공적으로 생성되었습니다.', 'success');

        // 스케줄 목록 새로고침
        loadSchedules();

    } catch (error) {
        console.error('스케줄 생성 오류:', error);
        // 오류 메시지 구체적으로 표시
        if (error.message) {
            showToast(`스케줄 생성 오류: ${error.message}`, 'error');
        } else {
            showToast('스케줄 생성 중 오류가 발생했습니다.', 'error');
        }
    }
} 
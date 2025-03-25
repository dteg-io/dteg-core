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

        tbody.innerHTML = schedules.map(schedule => {
            // id와 pipeline_id가 undefined인 경우 기본값 사용
            const id = schedule.id || 'unknown';
            const pipelineId = schedule.pipeline_id || 'unknown';

            return `
            <tr>
                <td>${id.substring(0, 8)}...</td>
                <td>${pipelineId.substring(0, 8)}...</td>
                <td><code>${schedule.cron_expression || 'unknown'}</code></td>
                <td>${schedule.enabled ?
                    '<span class="badge bg-success">활성</span>' :
                    '<span class="badge bg-secondary">비활성</span>'}</td>
                <td>${formatDate(schedule.next_run)}</td>
                <td>
                    <button class="btn btn-sm btn-primary run-schedule" data-id="${id}">실행</button>
                    <button class="btn btn-sm btn-info edit-schedule" data-id="${id}">편집</button>
                    <button class="btn btn-sm btn-danger delete-schedule" data-id="${id}">삭제</button>
                </td>
            </tr>
        `}).join('');

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
async function editSchedule(scheduleId) {
    try {
        // 스케줄 정보와 파이프라인 목록을 동시에 가져옴
        const [schedule, pipelines] = await Promise.all([
            DtegApi.getSchedule(scheduleId),
            DtegApi.getPipelines()
        ]);

        // 파이프라인 목록 옵션 생성
        const pipelineOptions = pipelines.map(p =>
            `<option value="${p.id}" ${p.id === schedule.pipeline_id ? 'selected' : ''}>${p.name}</option>`
        ).join('');

        // 기존에 모달이 있으면 제거
        const existingModal = document.getElementById('schedule-modal');
        if (existingModal) {
            existingModal.remove();
        }

        // 모달 HTML 생성
        const modalHtml = `
        <div class="modal fade" id="schedule-modal" tabindex="-1">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title">스케줄 편집</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <div class="modal-body">
                        <form id="schedule-form">
                            <input type="hidden" id="schedule-id" value="${schedule.id}">
                            <div class="mb-3">
                                <label for="schedule-name" class="form-label">스케줄 이름</label>
                                <input type="text" class="form-control" id="schedule-name" value="${schedule.name}" required>
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
                                        value="${schedule.cron_expression}" required>
                                    <button class="btn btn-outline-secondary" type="button" id="cron-help-btn"
                                        title="Cron 표현식 도움말">?</button>
                                    <button class="btn btn-outline-primary" type="button" id="validate-cron-btn">검증</button>
                                </div>
                                <div class="form-text">예시: 0 0 * * * (매일 자정), 0 */6 * * * (6시간마다)</div>
                            </div>
                            <div class="mb-3 form-check">
                                <input type="checkbox" class="form-check-input" id="enabled" ${schedule.enabled ? 'checked' : ''}>
                                <label class="form-check-label" for="enabled">활성화</label>
                            </div>
                            <div class="mb-3">
                                <label for="parameters" class="form-label">파라미터 (JSON)</label>
                                <div class="input-group">
                                    <textarea class="form-control" id="parameters" rows="3" 
                                        placeholder='{"param1": "value1"}'>${schedule.parameters ? JSON.stringify(schedule.parameters, null, 2) : ''}</textarea>
                                    <button class="btn btn-outline-primary" type="button" id="validate-params-btn" 
                                        style="height: fit-content; align-self: start; margin-top: 0;">검증</button>
                                </div>
                                <div class="form-text">파이프라인 실행 시 전달할 파라미터를 JSON 형식으로 입력하세요.</div>
                            </div>
                        </form>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">취소</button>
                        <button type="button" class="btn btn-primary" id="update-schedule-btn">저장</button>
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

        // 검증 버튼 이벤트 연결
        document.getElementById('validate-cron-btn').addEventListener('click', validateCron);
        document.getElementById('validate-params-btn').addEventListener('click', validateParameterInput);

        // 저장 버튼에 이벤트 핸들러 연결
        document.getElementById('update-schedule-btn').addEventListener('click', updateSchedule);
    } catch (error) {
        console.error('스케줄 정보 로드 오류:', error);
        showToast('스케줄 정보를 불러오는 중 오류가 발생했습니다.', 'error');
    }
}

// 스케줄 업데이트 처리
async function updateSchedule() {
    try {
        // 폼 데이터 수집
        const scheduleId = document.getElementById('schedule-id').value;
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

        // Cron 표현식 검증
        const cronValidation = validateCronExpression(cronExpression);
        if (!cronValidation.valid) {
            showToast(`Cron 표현식 오류: ${cronValidation.error}`, 'error');
            return;
        }

        // 파라미터 JSON 파싱 (입력이 있는 경우)
        let parameters = null;
        const parametersText = document.getElementById('parameters').value.trim();
        if (parametersText) {
            try {
                parameters = JSON.parse(parametersText);

                // 파라미터 유효성 검사
                const paramsValidation = validateParameters(parameters);
                if (!paramsValidation.valid) {
                    showToast(`파라미터 오류: ${paramsValidation.error}`, 'error');
                    return;
                }
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
        await DtegApi.updateSchedule(scheduleId, scheduleData);

        // 모달 닫기
        const scheduleModal = bootstrap.Modal.getInstance(document.getElementById('schedule-modal'));
        scheduleModal.hide();

        // 성공 메시지 표시
        showToast('스케줄이 성공적으로 업데이트되었습니다.', 'success');

        // 스케줄 목록 새로고침
        loadSchedules();

    } catch (error) {
        console.error('스케줄 업데이트 오류:', error);
        // 오류 메시지 구체적으로 표시
        if (error.message) {
            showToast(`스케줄 업데이트 오류: ${error.message}`, 'error');
        } else {
            showToast('스케줄 업데이트 중 오류가 발생했습니다.', 'error');
        }
    }
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
                                    <button class="btn btn-outline-primary" type="button" id="validate-cron-btn">검증</button>
                                </div>
                                <div class="form-text">예시: 0 0 * * * (매일 자정), 0 */6 * * * (6시간마다)</div>
                            </div>
                            <div class="mb-3 form-check">
                                <input type="checkbox" class="form-check-input" id="enabled" checked>
                                <label class="form-check-label" for="enabled">활성화</label>
                            </div>
                            <div class="mb-3">
                                <label for="parameters" class="form-label">파라미터 (JSON)</label>
                                <div class="input-group">
                                    <textarea class="form-control" id="parameters" rows="3" 
                                        placeholder='{"param1": "value1"}'></textarea>
                                    <button class="btn btn-outline-primary" type="button" id="validate-params-btn" 
                                        style="height: fit-content; align-self: start; margin-top: 0;">검증</button>
                                </div>
                                <div class="form-text">파이프라인 실행 시 전달할 파라미터를 JSON 형식으로 입력하세요.</div>
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

        // 검증 버튼 이벤트 연결
        document.getElementById('validate-cron-btn').addEventListener('click', validateCron);
        document.getElementById('validate-params-btn').addEventListener('click', validateParameterInput);

        // 저장 버튼 이벤트
        document.getElementById('save-schedule-btn').addEventListener('click', createSchedule);
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

        // Cron 표현식 검증
        const cronValidation = validateCronExpression(cronExpression);
        if (!cronValidation.valid) {
            showToast(`Cron 표현식 오류: ${cronValidation.error}`, 'error');
            return;
        }

        // 파라미터 JSON 파싱 (입력이 있는 경우)
        let parameters = null;
        const parametersText = document.getElementById('parameters').value.trim();
        if (parametersText) {
            try {
                parameters = JSON.parse(parametersText);

                // 파라미터 유효성 검사
                const paramsValidation = validateParameters(parameters);
                if (!paramsValidation.valid) {
                    showToast(`파라미터 오류: ${paramsValidation.error}`, 'error');
                    return;
                }
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

// Cron 표현식 유효성 검사
function validateCronExpression(expression) {
    // 기본 형식 확인 (5개 필드)
    const parts = expression.trim().split(/\s+/);
    if (parts.length !== 5) {
        return {
            valid: false,
            error: "Cron 표현식은 5개 필드(분 시 일 월 요일)로 구성되어야 합니다."
        };
    }

    // 각 필드 범위 검사를 위한 정규식
    const patterns = [
        /^(\*|([0-9]|[1-5][0-9])(-([0-9]|[1-5][0-9]))?(\/[0-9]+)?)(,(\*|([0-9]|[1-5][0-9])(-([0-9]|[1-5][0-9]))?(\/[0-9]+)?))*$/, // 분 (0-59)
        /^(\*|([0-9]|1[0-9]|2[0-3])(-([0-9]|1[0-9]|2[0-3]))?(\/[0-9]+)?)(,(\*|([0-9]|1[0-9]|2[0-3])(-([0-9]|1[0-9]|2[0-3]))?(\/[0-9]+)?))*$/, // 시 (0-23)
        /^(\*|([1-9]|[12][0-9]|3[01])(-([1-9]|[12][0-9]|3[01]))?(\/[0-9]+)?)(,(\*|([1-9]|[12][0-9]|3[01])(-([1-9]|[12][0-9]|3[01]))?(\/[0-9]+)?))*$/, // 일 (1-31)
        /^(\*|([1-9]|1[0-2])(-([1-9]|1[0-2]))?(\/[0-9]+)?)(,(\*|([1-9]|1[0-2])(-([1-9]|1[0-2]))?(\/[0-9]+)?))*$/, // 월 (1-12)
        /^(\*|([0-6])(-([0-6]))?(\/[0-9]+)?)(,(\*|([0-6])(-([0-6]))?(\/[0-9]+)?))*$/ // 요일 (0-6)
    ];

    // 각 필드 검사
    const fieldNames = ["분", "시", "일", "월", "요일"];
    for (let i = 0; i < 5; i++) {
        if (!patterns[i].test(parts[i])) {
            return {
                valid: false,
                error: `${fieldNames[i]} 필드가 유효하지 않습니다: ${parts[i]}`
            };
        }
    }

    return { valid: true };
}

// 파라미터 JSON 유효성 검사
function validateParameters(params) {
    // 파라미터가 없는 경우
    if (!params) {
        return { valid: true };
    }

    // 객체 타입인지 확인
    if (typeof params !== 'object' || Array.isArray(params)) {
        return {
            valid: false,
            error: "파라미터는 JSON 객체 형식이어야 합니다."
        };
    }

    // 값의 타입 확인 (문자열, 숫자, 불리언만 허용)
    for (const [key, value] of Object.entries(params)) {
        const type = typeof value;
        if (type !== 'string' && type !== 'number' && type !== 'boolean' && value !== null) {
            return {
                valid: false,
                error: `파라미터 '${key}'의 값이 유효하지 않습니다. 문자열, 숫자, 불리언, null만 허용됩니다.`
            };
        }
    }

    return { valid: true };
}

// Cron 검증 함수 (사용자 인터페이스용)
function validateCron() {
    const cronExpression = document.getElementById('cron-expression').value.trim();
    if (!cronExpression) {
        showToast('Cron 표현식을 입력해주세요.', 'error');
        return;
    }

    const validation = validateCronExpression(cronExpression);
    if (!validation.valid) {
        showToast(`Cron 표현식 오류: ${validation.error}`, 'error');
    } else {
        showToast('Cron 표현식이 유효합니다.', 'success');
    }
}

// 파라미터 검증 함수 (사용자 인터페이스용)
function validateParameterInput() {
    const parametersText = document.getElementById('parameters').value.trim();
    if (!parametersText) {
        showToast('파라미터가 비어 있습니다.', 'info');
        return;
    }

    try {
        const parameters = JSON.parse(parametersText);
        const validation = validateParameters(parameters);

        if (!validation.valid) {
            showToast(`파라미터 오류: ${validation.error}`, 'error');
        } else {
            showToast('파라미터가 유효합니다.', 'success');
        }
    } catch (e) {
        showToast(`JSON 형식 오류: ${e.message}`, 'error');
    }
} 
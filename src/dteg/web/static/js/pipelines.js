/**
 * DTEG Web UI - 파이프라인 관련 기능
 */

// 파이프라인 목록 로드
async function loadPipelines() {
    const tbody = pageHandlers.pipelines.elements.tbody;
    clearTable(tbody);

    try {
        const pipelines = await DtegApi.getPipelines();

        if (pipelines.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" class="text-center">파이프라인이 없습니다</td></tr>';
            return;
        }

        tbody.innerHTML = pipelines.map(pipeline => `
            <tr>
                <td>${pipeline.id.substring(0, 8)}...</td>
                <td>${pipeline.name}</td>
                <td>${pipeline.description || '-'}</td>
                <td>${formatDate(pipeline.created_at)}</td>
                <td>
                    <button class="btn btn-sm btn-primary run-pipeline" data-id="${pipeline.id}">실행</button>
                    <button class="btn btn-sm btn-info edit-pipeline" data-id="${pipeline.id}">편집</button>
                    <button class="btn btn-sm btn-danger delete-pipeline" data-id="${pipeline.id}">삭제</button>
                </td>
            </tr>
        `).join('');

        // 버튼 이벤트 바인딩
        tbody.querySelectorAll('.run-pipeline').forEach(btn => {
            btn.addEventListener('click', () => runPipeline(btn.dataset.id));
        });

        tbody.querySelectorAll('.edit-pipeline').forEach(btn => {
            btn.addEventListener('click', () => editPipeline(btn.dataset.id));
        });

        tbody.querySelectorAll('.delete-pipeline').forEach(btn => {
            btn.addEventListener('click', () => deletePipeline(btn.dataset.id));
        });
    } catch (error) {
        console.error('파이프라인 로드 오류:', error);
        tbody.innerHTML = '<tr><td colspan="5" class="text-center text-danger">파이프라인을 불러오는 중 오류가 발생했습니다</td></tr>';
    }
}

// 파이프라인 실행
async function runPipeline(pipelineId) {
    try {
        await DtegApi.runPipeline(pipelineId);
        showToast('파이프라인 실행이 예약되었습니다', 'success');
    } catch (error) {
        showToast('파이프라인 실행 중 오류가 발생했습니다', 'error');
    }
}

// 파이프라인 편집
function editPipeline(pipelineId) {
    // 구현 예정
    alert(`파이프라인 ID ${pipelineId} 편집 기능은 구현 예정입니다.`);
}

// 파이프라인 삭제
async function deletePipeline(pipelineId) {
    if (!confirm('정말로 이 파이프라인을 삭제하시겠습니까?')) {
        return;
    }

    try {
        await DtegApi.deletePipeline(pipelineId);
        showToast('파이프라인이 삭제되었습니다', 'success');
        loadPipelines();
    } catch (error) {
        showToast('파이프라인 삭제 중 오류가 발생했습니다', 'error');
    }
}

// 파이프라인 생성 모달 표시
function showCreatePipelineModal() {
    // 기존에 모달이 있으면 제거
    const existingModal = document.getElementById('pipeline-modal');
    if (existingModal) {
        existingModal.remove();
    }

    // 모달 HTML 생성
    const modalHtml = `
    <div class="modal fade" id="pipeline-modal" tabindex="-1">
        <div class="modal-dialog modal-lg">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title">새 파이프라인 생성</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                </div>
                <div class="modal-body">
                    <form id="pipeline-form">
                        <div class="mb-3">
                            <label for="pipeline-name" class="form-label">파이프라인 이름</label>
                            <input type="text" class="form-control" id="pipeline-name" required>
                        </div>
                        <div class="mb-3">
                            <label for="pipeline-description" class="form-label">설명</label>
                            <textarea class="form-control" id="pipeline-description" rows="2"></textarea>
                        </div>
                        <div class="mb-3">
                            <label for="pipeline-config" class="form-label">파이프라인 설정 (JSON)</label>
                            <textarea class="form-control" id="pipeline-config" rows="12" 
                                required placeholder='{"source": {"type": "mysql"}, "destination": {"type": "csv"}}'></textarea>
                            <div class="form-text">
                                파이프라인 설정을 JSON 형식으로 입력하세요.
                                <a href="#" id="show-template-btn">템플릿 보기</a>
                            </div>
                        </div>
                    </form>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">취소</button>
                    <button type="button" class="btn btn-primary" id="save-pipeline-btn">저장</button>
                </div>
            </div>
        </div>
    </div>
    `;

    // 모달을 DOM에 추가
    document.body.insertAdjacentHTML('beforeend', modalHtml);

    // 모달 인스턴스 생성 및 표시
    const pipelineModal = new bootstrap.Modal(document.getElementById('pipeline-modal'));
    pipelineModal.show();

    // 템플릿 보기 버튼 이벤트
    document.getElementById('show-template-btn').addEventListener('click', (e) => {
        e.preventDefault();
        const templateConfig = {
            "name": "example-pipeline",
            "description": "MySQL에서 CSV로 데이터 추출",
            "source": {
                "type": "mysql",
                "config": {
                    "host": "localhost",
                    "port": 3306,
                    "database": "example_db",
                    "user": "dbuser",
                    "password": "dbpassword",
                    "table": "source_table",
                    "query": "SELECT * FROM source_table LIMIT 1000"
                }
            },
            "destination": {
                "type": "csv",
                "config": {
                    "path": "/path/to/output.csv",
                    "delimiter": ",",
                    "encoding": "utf-8"
                }
            },
            "variables": {
                "batch_size": 1000,
                "max_rows": 10000
            }
        };

        document.getElementById('pipeline-config').value = JSON.stringify(templateConfig, null, 2);
    });
}

// 파이프라인 생성 처리
async function createPipeline() {
    try {
        // 폼 데이터 수집
        const name = document.getElementById('pipeline-name').value;
        const description = document.getElementById('pipeline-description').value;

        // 유효성 검사
        if (!name) {
            showToast('파이프라인 이름을 입력해주세요.', 'error');
            return;
        }

        // 설정 JSON 파싱
        let config = null;
        try {
            const configText = document.getElementById('pipeline-config').value.trim();
            if (!configText) {
                showToast('파이프라인 설정을 입력해주세요.', 'error');
                return;
            }
            config = JSON.parse(configText);
        } catch (e) {
            showToast('파이프라인 설정 JSON 형식이 올바르지 않습니다.', 'error');
            return;
        }

        // 파이프라인 데이터 생성
        const pipelineData = {
            name,
            description,
            config
        };

        // API 호출
        await DtegApi.createPipeline(pipelineData);

        // 모달 닫기
        const pipelineModal = bootstrap.Modal.getInstance(document.getElementById('pipeline-modal'));
        pipelineModal.hide();

        // 성공 메시지 표시
        showToast('파이프라인이 성공적으로 생성되었습니다.', 'success');

        // 파이프라인 목록 새로고침
        loadPipelines();

    } catch (error) {
        console.error('파이프라인 생성 오류:', error);
        // 오류 메시지 구체적으로 표시
        if (error.message) {
            showToast(`파이프라인 생성 오류: ${error.message}`, 'error');
        } else {
            showToast('파이프라인 생성 중 오류가 발생했습니다.', 'error');
        }
    }
} 
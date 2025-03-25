/**
 * DTEG API 클라이언트
 * API 엔드포인트 호출을 처리하는 모듈
 */

// API 기본 경로
const API_BASE_URL = '/api';

// API 클라이언트 클래스
class DtegApi {
    // 토큰 관리
    static getToken() {
        return localStorage.getItem('auth_token');
    }

    static setToken(token) {
        localStorage.setItem('auth_token', token);
    }

    static clearToken() {
        localStorage.removeItem('auth_token');
    }

    static isAuthenticated() {
        return !!this.getToken();
    }

    // API 호출 기본 함수
    static async fetchApi(endpoint, options = {}) {
        const url = `${API_BASE_URL}${endpoint}`;

        // 기본 헤더 설정
        const headers = {
            'Content-Type': 'application/json',
            ...options.headers
        };

        // 인증 토큰 추가
        if (this.isAuthenticated()) {
            headers['Authorization'] = `Bearer ${this.getToken()}`;
        }

        // 요청 옵션 구성
        const fetchOptions = {
            ...options,
            headers
        };

        try {
            const response = await fetch(url, fetchOptions);

            // 인증 오류 처리
            if (response.status === 401) {
                this.clearToken();
                // 인증되지 않은 경우 환영 페이지로 이동
                if (typeof showWelcomePage === 'function') {
                    showWelcomePage();
                }
                throw new Error('인증이 필요합니다');
            }

            // 204 No Content 응답 처리 (주로 DELETE 요청)
            if (response.status === 204) {
                return {}; // 빈 객체 반환
            }

            // JSON 응답 파싱
            const data = await response.json();

            // 오류 응답 처리
            if (!response.ok) {
                // 오류 메시지 상세 정보 추출
                let errorMessage = '요청 처리 중 오류가 발생했습니다';

                if (data.detail) {
                    errorMessage = data.detail;
                } else if (typeof data === 'object' && Object.keys(data).length > 0) {
                    // 유효성 검사 오류 처리 (필드별 오류 메시지)
                    const fieldErrors = [];
                    for (const [field, errors] of Object.entries(data)) {
                        if (Array.isArray(errors)) {
                            fieldErrors.push(`${field}: ${errors.join(', ')}`);
                        } else if (typeof errors === 'string') {
                            fieldErrors.push(`${field}: ${errors}`);
                        }
                    }

                    if (fieldErrors.length > 0) {
                        errorMessage = fieldErrors.join('\n');
                    }
                }

                const error = new Error(errorMessage);
                error.status = response.status;
                error.data = data;
                throw error;
            }

            return data;
        } catch (error) {
            console.error('API 요청 오류:', error);
            throw error;
        }
    }

    // 인증 API
    static async login(username, password) {
        const formData = new FormData();
        formData.append('username', username);
        formData.append('password', password);

        const response = await fetch(`${API_BASE_URL}/auth/token`, {
            method: 'POST',
            body: formData
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || '로그인에 실패했습니다');
        }

        const data = await response.json();
        this.setToken(data.access_token);
        return data;
    }

    static async getCurrentUser() {
        return this.fetchApi('/auth/me');
    }

    // 파이프라인 API
    static async getPipelines() {
        return this.fetchApi('/pipelines');
    }

    static async getPipeline(pipelineId) {
        return this.fetchApi(`/pipelines/${pipelineId}`);
    }

    static async createPipeline(pipelineData) {
        return this.fetchApi('/pipelines', {
            method: 'POST',
            body: JSON.stringify(pipelineData)
        });
    }

    static async updatePipeline(pipelineId, pipelineData) {
        return this.fetchApi(`/pipelines/${pipelineId}`, {
            method: 'PUT',
            body: JSON.stringify(pipelineData)
        });
    }

    static async deletePipeline(pipelineId) {
        return this.fetchApi(`/pipelines/${pipelineId}`, {
            method: 'DELETE'
        });
    }

    static async runPipeline(pipelineId) {
        return this.fetchApi(`/pipelines/${pipelineId}/run`, {
            method: 'POST'
        });
    }

    // 스케줄 API
    static async getSchedules() {
        return this.fetchApi('/schedules');
    }

    static async getSchedule(scheduleId) {
        return this.fetchApi(`/schedules/${scheduleId}`);
    }

    static async createSchedule(scheduleData) {
        return this.fetchApi('/schedules', {
            method: 'POST',
            body: JSON.stringify(scheduleData)
        });
    }

    static async updateSchedule(scheduleId, scheduleData) {
        return this.fetchApi(`/schedules/${scheduleId}`, {
            method: 'PUT',
            body: JSON.stringify(scheduleData)
        });
    }

    static async deleteSchedule(scheduleId) {
        return this.fetchApi(`/schedules/${scheduleId}`, {
            method: 'DELETE'
        });
    }

    static async runSchedule(scheduleId) {
        return this.fetchApi(`/schedules/${scheduleId}/run`, {
            method: 'POST'
        });
    }

    // 실행 이력 API
    static async getExecutions(page = 1, pageSize = 10, filters = {}) {
        const queryParams = new URLSearchParams({
            page,
            page_size: pageSize,
            ...filters
        });

        return this.fetchApi(`/executions?${queryParams}`);
    }

    static async getExecution(executionId) {
        return this.fetchApi(`/executions/${executionId}`);
    }

    static async getExecutionLogs(executionId) {
        const response = await fetch(`${API_BASE_URL}/executions/${executionId}/logs`, {
            headers: {
                'Authorization': `Bearer ${this.getToken()}`
            }
        });

        if (!response.ok) {
            throw new Error('로그를 가져오는 중 오류가 발생했습니다');
        }

        return response.text();
    }

    static async deleteExecution(executionId) {
        return this.fetchApi(`/executions/${executionId}`, {
            method: 'DELETE'
        });
    }

    // 시스템 API
    static async getHealthStatus() {
        return this.fetchApi('/health');
    }

    static async getSystemConfig() {
        return this.fetchApi('/config');
    }

    // 대시보드 API
    static async getDashboardMetrics() {
        try {
            // 백엔드 API를 통해 직접 메트릭을 가져옵니다.
            console.log('대시보드 메트릭 API 요청 시작');
            const metricsResponse = await this.fetchApi('/dashboard/metrics');
            console.log('대시보드 메트릭 API 응답 원본:', metricsResponse);

            // 백엔드 API가 제대로 응답하는 경우 해당 값을 사용
            if (metricsResponse && metricsResponse.pipeline_status) {
                console.log('백엔드 메트릭 응답 사용, 완료된 실행 수:', metricsResponse.pipeline_status.completed);
                return metricsResponse;
            }

            // 직접 실행 이력을 모두 가져와서 계산합니다.
            console.log('백엔드 메트릭 응답이 없거나 형식이 맞지 않음, 실행 이력 데이터로 계산 시작');
            const allExecutions = await this.fetchApi('/executions?page=1&page_size=100');
            console.log('전체 실행 이력 API 응답:', allExecutions);

            let total = 0;
            let executions = [];

            // 응답 구조에 따라 데이터 추출
            if (Array.isArray(allExecutions)) {
                executions = allExecutions;
                total = allExecutions.length;
            } else if (allExecutions.executions && Array.isArray(allExecutions.executions)) {
                executions = allExecutions.executions;
                total = allExecutions.total || executions.length;
            } else {
                total = allExecutions.total || 0;
            }

            console.log('전체 실행 이력 수 (정확한 값):', total);

            // 상태별 카운터 초기화
            let completed = 0;
            let failed = 0;
            let running = 0;
            let cancelled = 0;

            // 모든 실행 이력을 순회하며 상태별 카운트
            executions.forEach(execution => {
                const status = execution.status.toLowerCase();
                if (status === 'completed') completed++;
                else if (status === 'failed') failed++;
                else if (status === 'running') running++;
                else if (status === 'cancelled' || status === 'canceled') cancelled++;
            });

            console.log('정확한 상태별 카운트:', { completed, failed, running, cancelled });

            const pipelineStatus = {
                completed,
                failed,
                running,
                cancelled
            };

            console.log('계산된 메트릭 결과:', {
                total_executions: total,
                completed,
                failed,
                running,
                cancelled
            });

            return {
                pipeline_status: pipelineStatus,
                total_executions: total,
                recent_success_rate: (completed / (completed + failed) * 100) || 0
            };
        } catch (error) {
            console.error('메트릭 로드 오류:', error);
            return {
                pipeline_status: {
                    completed: 0,
                    failed: 0,
                    running: 0,
                    cancelled: 0
                },
                total_executions: 0,
                recent_success_rate: 0
            };
        }
    }

    static async getRecentExecutions(limit = 5) {
        // 최근 실행 이력은 기존 API로 가져옴 (limit으로 제한)
        const response = await this.fetchApi(`/executions?page=1&page_size=${limit}`);

        // 응답 구조에 따라 실행 목록 추출
        if (Array.isArray(response)) {
            return response;
        } else if (response.executions && Array.isArray(response.executions)) {
            return response.executions;
        }
        return [];
    }
} 
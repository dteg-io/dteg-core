/**
 * DTEG Web UI - 인증 관련 기능
 */

// 인증 관련
async function handleLogin(e) {
    e.preventDefault();

    const username = document.getElementById('username-input').value;
    const password = document.getElementById('password-input').value;

    try {
        await DtegApi.login(username, password);
        loginModal.hide();

        // 사용자 정보 로드
        const user = await DtegApi.getCurrentUser();
        usernameSpan.textContent = user.username;

        // 로그인 성공 시 대시보드로 이동
        showPage('dashboard');

        showToast('로그인에 성공했습니다', 'success');
    } catch (error) {
        showToast('로그인에 실패했습니다. 사용자명과 비밀번호를 확인해주세요', 'error');
    }
}

function handleLogout() {
    DtegApi.clearToken();
    usernameSpan.textContent = '사용자';

    // 로그아웃 시 환영 페이지로 이동
    showWelcomePage();

    showToast('로그아웃 되었습니다', 'info');
} 
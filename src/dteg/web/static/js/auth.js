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

        // 현재 페이지 새로고침
        const activePage = document.querySelector('.nav-link.active').dataset.page;
        showPage(activePage);

        showToast('로그인에 성공했습니다', 'success');
    } catch (error) {
        showToast('로그인에 실패했습니다. 사용자명과 비밀번호를 확인해주세요', 'error');
    }
}

function handleLogout() {
    DtegApi.clearToken();
    usernameSpan.textContent = '사용자';
    showLoginModal();
    showToast('로그아웃 되었습니다', 'info');
} 
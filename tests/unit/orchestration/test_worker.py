"""
Celery 워커 모듈 단위 테스트
"""
import unittest
from unittest.mock import patch, MagicMock, call
import tempfile
import os
from pathlib import Path

from dteg.orchestration.worker import CeleryTaskQueue, setup_celery, pipeline_task


class TestCeleryTaskQueue(unittest.TestCase):
    """Celery 태스크 큐 클래스 테스트"""
    
    @patch('dteg.orchestration.worker.setup_celery')
    def setUp(self, mock_setup_celery):
        """테스트 설정"""
        # Celery 앱 모의 객체
        self.mock_app = MagicMock()
        mock_setup_celery.return_value = self.mock_app
        
        # 콜백 함수 모의 객체
        self.mock_callback = MagicMock()
        
        # 태스크 큐 생성
        self.task_queue = CeleryTaskQueue(
            broker_url="redis://localhost:6379/0",
            result_backend="redis://localhost:6379/1",
            on_task_complete=self.mock_callback
        )
    
    def test_init(self):
        """초기화 테스트"""
        self.assertEqual(self.task_queue.broker_url, "redis://localhost:6379/0")
        self.assertEqual(self.task_queue.result_backend, "redis://localhost:6379/1")
        self.assertEqual(self.task_queue.on_task_complete, self.mock_callback)
        self.assertEqual(self.task_queue.celery_app, self.mock_app)
    
    @patch('dteg.orchestration.worker.pipeline_task')
    def test_run_pipeline(self, mock_pipeline_task):
        """파이프라인 실행 테스트"""
        # 비동기 태스크 모의 객체
        mock_async_result = MagicMock()
        mock_pipeline_task.delay.return_value = mock_async_result
        
        # 테스트 데이터
        pipeline_config = MagicMock()
        execution_id = "execution-123"
        
        # 파이프라인 실행
        task_id = self.task_queue.run_pipeline(pipeline_config, execution_id)
        
        # pipeline_task.delay 호출 확인
        mock_pipeline_task.delay.assert_called_once_with(pipeline_config, execution_id)
        
        # 태스크 ID 반환 확인
        self.assertEqual(task_id, mock_async_result.id)
    
    def test_get_task_status_pending(self):
        """대기 중인 태스크 상태 조회"""
        # AsyncResult 모의 객체
        mock_async_result = MagicMock()
        mock_async_result.state = "PENDING"
        mock_async_result.ready.return_value = False
        self.mock_app.AsyncResult.return_value = mock_async_result
        
        # 태스크 상태 조회
        status = self.task_queue.get_task_status("task-123")
        
        # Celery.AsyncResult 호출 확인
        self.mock_app.AsyncResult.assert_called_once_with("task-123")
        
        # 상태 확인
        self.assertEqual(status, "PENDING")
    
    def test_get_task_status_success(self):
        """성공적으로 완료된 태스크 상태 조회"""
        # AsyncResult 모의 객체
        mock_async_result = MagicMock()
        mock_async_result.state = "SUCCESS"
        mock_async_result.ready.return_value = True
        mock_async_result.successful.return_value = True
        self.mock_app.AsyncResult.return_value = mock_async_result
        
        # 태스크 상태 조회
        status = self.task_queue.get_task_status("task-123")
        
        # 상태 확인
        self.assertEqual(status, "SUCCESS")
    
    def test_get_task_status_failure(self):
        """실패한 태스크 상태 조회"""
        # AsyncResult 모의 객체
        mock_async_result = MagicMock()
        mock_async_result.state = "FAILURE"
        mock_async_result.ready.return_value = True
        mock_async_result.successful.return_value = False
        self.mock_app.AsyncResult.return_value = mock_async_result
        
        # 태스크 상태 조회
        status = self.task_queue.get_task_status("task-123")
        
        # 상태 확인
        self.assertEqual(status, "FAILURE")
    
    def test_cancel_task(self):
        """태스크 취소"""
        # AsyncResult 모의 객체
        mock_async_result = MagicMock()
        self.mock_app.AsyncResult.return_value = mock_async_result
        
        # 태스크 취소
        result = self.task_queue.cancel_task("task-123")
        
        # revoke 메소드 호출 확인
        self.mock_app.control.revoke.assert_called_once_with("task-123", terminate=True)
        
        # 결과 확인
        self.assertTrue(result)
    
    def test_cancel_task_failure(self):
        """태스크 취소 실패"""
        # AsyncResult 모의 객체
        mock_async_result = MagicMock()
        self.mock_app.AsyncResult.return_value = mock_async_result
        
        # revoke 메소드가 예외를 던지도록 설정
        self.mock_app.control.revoke.side_effect = Exception("취소 실패")
        
        # 태스크 취소 시도
        result = self.task_queue.cancel_task("task-123")
        
        # 결과 확인 (실패)
        self.assertFalse(result)


@patch('dteg.orchestration.worker.Celery')
def test_setup_celery(mock_celery_class):
    """Celery 설정 함수 테스트"""
    # Celery 모의 객체
    mock_app = MagicMock()
    mock_celery_class.return_value = mock_app
    
    # 브로커 및 결과 백엔드 URL
    broker_url = "redis://localhost:6379/0"
    result_backend = "redis://localhost:6379/1"
    
    # Celery 설정
    app = setup_celery(broker_url, result_backend)
    
    # Celery 클래스 생성자 호출 확인
    mock_celery_class.assert_called_once_with(
        "dteg_worker",
        broker=broker_url,
        backend=result_backend
    )
    
    # 설정 적용 확인
    mock_app.conf.update.assert_called_once_with(
        task_serializer="pickle",
        accept_content=["pickle"],
        result_serializer="pickle"
    )
    
    # 반환값 확인
    assert app == mock_app


@patch('dteg.orchestration.worker.Pipeline')
def test_pipeline_task(mock_pipeline_class):
    """파이프라인 태스크 함수 테스트"""
    # Pipeline 인스턴스 모의 객체
    mock_pipeline = MagicMock()
    mock_pipeline_class.return_value = mock_pipeline
    
    # 테스트 데이터
    pipeline_config = MagicMock()
    execution_id = "execution-123"
    
    # 파이프라인 태스크 실행
    result = pipeline_task(pipeline_config, execution_id)
    
    # Pipeline 클래스 생성자 호출 확인
    mock_pipeline_class.assert_called_once_with(pipeline_config)
    
    # Pipeline.run 메소드 호출 확인
    mock_pipeline.run.assert_called_once()
    
    # 결과 확인
    assert result == {
        "execution_id": execution_id,
        "success": True,
        "error": None
    }


@patch('dteg.orchestration.worker.Pipeline')
def test_pipeline_task_failure(mock_pipeline_class):
    """파이프라인 태스크 실패 테스트"""
    # Pipeline 인스턴스 모의 객체
    mock_pipeline = MagicMock()
    mock_pipeline_class.return_value = mock_pipeline
    
    # Pipeline.run 메소드가 예외를 던지도록 설정
    error_message = "파이프라인 실행 실패"
    mock_pipeline.run.side_effect = Exception(error_message)
    
    # 테스트 데이터
    pipeline_config = MagicMock()
    execution_id = "execution-123"
    
    # 파이프라인 태스크 실행
    result = pipeline_task(pipeline_config, execution_id)
    
    # 결과 확인 (실패)
    assert result == {
        "execution_id": execution_id,
        "success": False,
        "error": f"Error: {error_message}"
    }


if __name__ == "__main__":
    unittest.main() 
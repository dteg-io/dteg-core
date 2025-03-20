"""
DbtTransformer 단위 테스트
"""
import os
import subprocess
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

from dteg.transformers.dbt import DbtTransformer


@pytest.fixture
def mock_subprocess_run():
    """subprocess.run 호출을 모킹하는 fixture"""
    with patch("dteg.transformers.dbt.subprocess.run") as mock_run:
        # dbt 명령 실행 모킹
        process_mock = MagicMock()
        process_mock.returncode = 0
        process_mock.stdout = "dbt 실행 성공"
        mock_run.return_value = process_mock
        yield mock_run


@pytest.fixture
def mock_path_exists():
    """os.path.exists 호출을 모킹하는 fixture"""
    with patch("os.path.exists") as mock_exists:
        mock_exists.return_value = True
        yield mock_exists


@pytest.fixture
def sample_csv_file(tmp_path):
    """테스트용 CSV 파일 생성"""
    csv_path = tmp_path / "test_result.csv"
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'value': [100, 200, 300]
    })
    df.to_csv(csv_path, index=False)
    return str(csv_path)


def test_dbt_transformer_init_validation():
    """DbtTransformer 초기화 검증 테스트"""
    # 필수 인자가 없는 경우
    with pytest.raises(ValueError):
        DbtTransformer({})
    
    with pytest.raises(ValueError):
        DbtTransformer({"project_dir": "/path/to/project"})
    
    with pytest.raises(ValueError):
        DbtTransformer({"result_path": "/path/to/result"})


@patch("os.path.exists")
@patch("dteg.transformers.dbt.subprocess.run")
def test_dbt_transformer_initialize(mock_run, mock_exists):
    """DbtTransformer 초기화 테스트"""
    # 성공 케이스
    mock_exists.return_value = True
    mock_run.return_value = MagicMock(returncode=0)
    
    config = {
        "project_dir": "/path/to/dbt_project",
        "result_path": "/path/to/result",
        "profiles_dir": "/path/to/profiles",
        "target": "dev",
        "models": ["model1", "model2"],
        "full_refresh": True
    }
    
    transformer = DbtTransformer(config)
    
    assert transformer.project_dir == "/path/to/dbt_project"
    assert transformer.result_path == "/path/to/result"
    assert transformer.profiles_dir == "/path/to/profiles"
    assert transformer.target == "dev"
    assert transformer.models == ["model1", "model2"]
    assert transformer.full_refresh is True


@patch("dteg.transformers.dbt.subprocess.run")
@patch("os.path.exists")
def test_dbt_transformer_run_dbt_command(mock_exists, mock_run):
    """dbt 명령 실행 테스트"""
    mock_exists.return_value = True
    mock_run.return_value = MagicMock(returncode=0)
    
    config = {
        "project_dir": "/path/to/dbt_project",
        "result_path": "/path/to/result",
        "target": "dev",
        "models": "model1 model2",
        "full_refresh": True
    }
    
    transformer = DbtTransformer(config)
    
    # 초기화에서 이미 한번 호출된 mock_run 리셋
    mock_run.reset_mock()
    
    transformer._run_dbt()
    
    # 명령 실행 검증
    assert mock_run.call_count == 1
    cmd_args = mock_run.call_args[0][0]
    assert "dbt" in cmd_args
    assert "run" in cmd_args
    assert "--project-dir" in cmd_args
    assert "--target" in cmd_args
    assert "--models" in cmd_args
    assert "--full-refresh" in cmd_args


@patch("os.path.exists")
@patch("dteg.transformers.dbt.subprocess.run")
def test_dbt_transformer_run_failed(mock_run, mock_exists):
    """dbt 실행 실패 테스트"""
    mock_exists.return_value = True
    # 초기화는 성공하고 실행만 실패하도록 설정
    mock_run.side_effect = [
        MagicMock(returncode=0),  # initialize 성공
        subprocess.CalledProcessError(1, "dbt run", stderr="오류 발생")  # _run_dbt 실패
    ]
    
    config = {
        "project_dir": "/path/to/dbt_project",
        "result_path": "/path/to/result"
    }
    
    transformer = DbtTransformer(config)
    
    with pytest.raises(RuntimeError):
        transformer._run_dbt()


@patch("pandas.read_csv")
@patch("os.path.exists")
def test_get_results_from_csv(mock_exists, mock_read_csv):
    """CSV 결과 로드 테스트"""
    mock_exists.return_value = True
    expected_df = pd.DataFrame({
        'id': [1, 2, 3],
        'value': [100, 200, 300]
    })
    mock_read_csv.return_value = expected_df
    
    # subprocess.run을 모킹하여 DbtTransformer 초기화 허용
    with patch("dteg.transformers.dbt.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        
        config = {
            "project_dir": "/path/to/dbt_project",
            "result_path": "/path/to/result.csv",
            "result_source": "csv"
        }
        
        transformer = DbtTransformer(config)
        result = transformer._get_results()
        
        assert mock_read_csv.called
        pd.testing.assert_frame_equal(result, expected_df)


@patch("dteg.transformers.dbt.subprocess.run")
@patch("os.path.exists")
def test_transform(mock_exists, mock_run):
    """transform 메서드 테스트"""
    mock_exists.return_value = True
    mock_run.return_value = MagicMock(returncode=0)
    
    config = {
        "project_dir": "/path/to/dbt_project",
        "result_path": "/path/to/result.csv",
        "result_source": "csv"
    }
    
    with patch("pandas.read_csv") as mock_read_csv:
        expected_df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [100, 200, 300]
        })
        mock_read_csv.return_value = expected_df
        
        transformer = DbtTransformer(config)
        
        # 초기화에서 이미 한번 호출된 mock_run 리셋
        mock_run.reset_mock()
        
        input_df = pd.DataFrame()  # 입력은 무시됨
        result = transformer.transform(input_df)
        
        pd.testing.assert_frame_equal(result, expected_df)
        assert mock_run.call_count == 1  # dbt run 명령 호출 확인 
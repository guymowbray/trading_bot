import pytest

from src.util.util import create_and_validate_s3_filepath


def test_create_and_validate_s3_filepath_successfully():
    execution_uuid = "20260322_203402_dcb571f4f795490b83eb63e47815e52d"

    expected_filepath = (
        "signals/equities/2026/03/22/20260322_203402_dcb571f4f795490b83eb63e47815e52d"
    )

    actual_filepath = create_and_validate_s3_filepath(
        base_dir="signals",
        market_data_type="equities",
        today_date="2026/03/22",
        execution_uuid=execution_uuid,
    )

    assert actual_filepath == expected_filepath


def test_create_and_validate_s3_filepath_raises_errordue_to_incorrect_today_date():
    incorrect_date_format = "11/03/22"

    with pytest.raises(ValueError):
        create_and_validate_s3_filepath(
            base_dir="signals",
            market_data_type="equities",
            today_date=incorrect_date_format,
            execution_uuid="20260322_203402_dcb571f4f795490b83eb63e47815e52d",
        )


def test_create_and_validate_s3_filepath_raises_error_due_to_incorrect_base_dir():
    incorrect_base_dir_format = "4378564398"

    with pytest.raises(ValueError):
        create_and_validate_s3_filepath(
            base_dir=incorrect_base_dir_format,
            market_data_type="equities",
            today_date="2026/03/22",
            execution_uuid="20260322_203402_dcb571f4f795490b83eb63e47815e52d",
        )

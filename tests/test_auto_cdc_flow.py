from common.auto_cdc import create_auto_cdc_flow


def test_create_auto_cdc_flow_is_function():
    assert callable(create_auto_cdc_flow)

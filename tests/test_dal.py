import pytest
import datetime
from app.dal import DataAccessLayer

def test_dal_save_and_retrieve_active_asset():
    """Validates full persistence routing loops inside your abstracted repository layer."""
    dal = DataAccessLayer()
    
    test_id = "TEST_STOCK_XYZ"
    asset_payload = {
        "assetId": test_id,
        "symbol": "XYZ",
        "asset_class": "stock",
        "description": "Verification Initial Frame Mock",
        "region_origin": "Europe"
    }
    
    # Run target persistence function
    dal.save_asset(asset_payload)
    
    # Confirm deterministic active latest retrieval layer semantics function correctly
    active_record = dal.get_active_asset(test_id)
    assert active_record is not None
    assert active_record["description"] == "Verification Initial Frame Mock"
    assert active_record["valid_to"] is None
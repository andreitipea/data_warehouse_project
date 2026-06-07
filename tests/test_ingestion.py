import pytest
from app.dal import DataAccessLayer

def test_time_series_batch_provenance_constraint():
    """Enforces constraint validations verifying historical provenance lineages."""
    dal = DataAccessLayer()
    
    invalid_batch = [{
        "assetId": "MOCK_AST",
        "dataSourceId": "NON_EXISTENT_VNDR",
        "timestamp": "2026-06-07T00:00:00Z",
        "indicators": {"close": 10.0}
    }]
    
    # The ingestion pipeline must reject metrics if the source vendor isn't registered
    with pytest.raises(ValueError):
        dal.save_time_series_batch(invalid_batch)
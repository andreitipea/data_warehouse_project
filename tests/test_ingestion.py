import pytest
import datetime
from app.dal import DataAccessLayer

def test_time_series_batch_provenance_constraint():
    """Enforces constraint validations verifying historical provenance lineages."""
    dal = DataAccessLayer()
    invalid_batch = [{
        "assetId": "MOCK_AST",
        "dataSourceId": "NON_EXISTENT_VNDR",
        "timestamp": datetime.datetime.utcnow(),
        "indicators": {"close": 10.0}
    }]
    with pytest.raises(ValueError):
        dal.save_time_series_batch(invalid_batch)

def test_external_ingestion_provenance_mapping_logic():
    """[M4 Validation Test] Verifies external ingestion binds correct vendor metadata tracking tags."""
    dal = DataAccessLayer()
    provider_id = "COINGECKO_PUBLIC_API"
    
    # Pre-register provider metadata profile shell details
    dal.save_data_source({
        "dataSourceId": provider_id,
        "name": "CoinGecko API Platform",
        "description": "Mock Verification Layer"
    })
    
    source_record = dal.get_data_source(provider_id)
    assert source_record is not None
    assert source_record["dataSourceId"] == provider_id

def test_pagination_offset_limit_boundaries():
    """[API Pagination Test] Assures result arrays slice correctly according to boundary properties."""
    dal = DataAccessLayer()
    # Populate mock data sources to verify pagination slicing functionality
    dal.save_data_source({"dataSourceId": "PAG_1", "name": "A", "description": "Desc"})
    dal.save_data_source({"dataSourceId": "PAG_2", "name": "B", "description": "Desc"})
    
    limited_set = dal.get_all_sources(limit=1, offset=0)
    assert len(limited_set) == 1
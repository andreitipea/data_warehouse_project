import datetime
import requests
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from app.dal import DataAccessLayer
from app.spark_engine import SparkAnalyticsEngine

app = FastAPI(title="Acme Ltd Financial Data Warehouse Pro", version="3.0.0")
dal = DataAccessLayer()
spark_engine = SparkAnalyticsEngine()

# --- Pydantic Data Verification Schemas ---

class DataSourceSchema(BaseModel):
    dataSourceId: str
    name: str
    description: str
    api_endpoint: Optional[str] = None

class FinancialAssetSchema(BaseModel):
    assetId: str
    symbol: str
    asset_class: str
    description: str
    region_origin: str
    additional_attributes: Dict[str, Any] = Field(default_factory=dict)

class TimeSeriesRecordSchema(BaseModel):
    assetId: str
    dataSourceId: str
    timestamp: datetime.datetime
    indicators: Dict[str, Any]

# --- UC 1: Data Ingest Controllers ---

@app.post("/api/v1/sources", tags=["Ingestion"])
def register_data_source(source: DataSourceSchema):
    success = dal.save_data_source(source.model_dump())
    if not success:
        raise HTTPException(status_code=400, detail="Data Source ID already registered.")
    return {"status": "Success", "message": f"Data Source {source.dataSourceId} saved."}

@app.post("/api/v1/assets", tags=["Ingestion"])
def upsert_financial_asset(asset: FinancialAssetSchema):
    result = dal.save_asset(asset.model_dump())
    return result

@app.delete("/api/v1/assets/{assetId}", tags=["Ingestion"])
def temporal_soft_delete(assetId: str):
    found = dal.soft_delete_asset(assetId)
    if not found:
        raise HTTPException(status_code=404, detail="Active asset entry not found.")
    return {"status": "Success", "message": "Soft delete marker recorded."}

@app.post("/api/v1/timeseries", tags=["Ingestion"])
def ingest_time_series_batch(records: List[TimeSeriesRecordSchema]):
    try:
        raw_records = [r.model_dump() for r in records]
        count = dal.save_time_series_batch(raw_records)
        return {"status": "Success", "records_ingested": count}
    except ValueError as err:
        raise HTTPException(status_code=400, detail=str(err))

# --- [CRITICAL FIX: REQUIREMENT M4] External Provider Live REST Engine ---

@app.post("/api/v1/ingest/external/crypto", tags=["Ingestion"])
def fetch_live_external_crypto_data(coin_id: str = "bitcoin"):
    """
    [Requirement M4] Connects to the public CoinGecko REST API, extracts market
    metrics, and logs data provenance lineage details directly inside the DWH store.
    """
    provider_id = "COINGECKO_PUBLIC_API"
    
    # 1. Enforce validation provenance metadata tracking checks
    if not dal.get_data_source(provider_id):
        dal.save_data_source({
            "dataSourceId": provider_id,
            "name": "CoinGecko API Platform",
            "description": "Public third-party REST feed for digital currencies."
        })
        
    # 2. Extract from external public endpoint
    external_url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd&include_24hr_vol=true"
    try:
        res = requests.get(external_url, timeout=10)
        data = res.json()
        
        if coin_id not in data:
            raise HTTPException(status_code=404, detail=f"Asset '{coin_id}' not found on external API host server.")
            
        metrics = data[coin_id]
        now = datetime.datetime.utcnow()
        asset_id = f"{coin_id.upper()}_CRYPTO"
        
        # 3. Ensure asset shell configuration profile is registered via temporal layer rules
        if not dal.get_active_asset(asset_id, as_of=now):
            dal.save_asset({
                "assetId": asset_id,
                "symbol": coin_id.upper()[:4],
                "asset_class": "crypto",
                "description": f"{coin_id.capitalize()} Digital Currency Core Asset",
                "region_origin": "Global",
                "additional_attributes": {"network": "Decentralized Native Ledger"}
            })
            
        # 4. Form and persist chronological metrics recording data source provenance
        timeseries_document = {
            "assetId": asset_id,
            "dataSourceId": provider_id,  # Link to data source for strict lineage tracking
            "timestamp": now,
            "indicators": {
                "close": float(metrics["usd"]),
                "volume_24h": float(metrics.get("usd_24h_vol", 0.0))
            }
        }
        
        dal.save_time_series_batch([timeseries_document])
        return {
            "status": "Success",
            "message": f"Successfully imported live asset metrics for {coin_id} via external REST provider.",
            "provenance_logged": timeseries_document
        }
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"External ingest handler failure: {str(err)}")

# --- UC 2: Consumption Controllers (With Pagination) ---

@app.get("/api/v1/assets", tags=["Data Consumption"])
def query_all_assets(limit: int = Query(10, ge=1, le=100), offset: int = Query(0, ge=0), as_of: Optional[str] = None):
    target_time = datetime.datetime.fromisoformat(as_of) if as_of else None
    return dal.get_all_assets(limit=limit, offset=offset, as_of=target_time)

@app.get("/api/v1/assets/{assetId}", tags=["Data Consumption"])
def query_asset_details(assetId: str, as_of: Optional[str] = None):
    target_time = datetime.datetime.fromisoformat(as_of) if as_of else None
    asset = dal.get_active_asset(assetId, as_of=target_time)
    if not asset:
        raise HTTPException(status_code=404, detail="Asset profile variant slice not found.")
    return asset

@app.get("/api/v1/sources", tags=["Data Consumption"])
def query_all_data_sources(limit: int = Query(10, ge=1, le=100), offset: int = Query(0, ge=0)):
    return dal.get_all_sources(limit=limit, offset=offset)

@app.get("/api/v1/sources/{dataSourceId}", tags=["Data Consumption"])
def query_data_source_details(dataSourceId: str):
    src = dal.get_data_source(dataSourceId)
    if not src:
        raise HTTPException(status_code=404, detail="Data source target entry not located.")
    return src

@app.get("/api/v1/timeseries", tags=["Data Consumption"])
def query_time_series(assetId: str, dataSourceId: str, start_date: Optional[str] = None, end_date: Optional[str] = None):
    start = datetime.datetime.fromisoformat(start_date) if start_date else None
    end = datetime.datetime.fromisoformat(end_date) if end_date else None
    return dal.get_time_series(assetId, dataSourceId, start, end)

# --- UC 3: Apache Spark Analytics & Persisted Pipelines ---

@app.get("/api/v1/analytics/aggregate", tags=["Analytics & Mining (Spark Powered)"])
def run_spark_aggregation(assetId: str, dataSourceId: str):
    records = dal.get_time_series(assetId, dataSourceId)
    if not records:
        raise HTTPException(status_code=404, detail="No historical records found to map Spark metrics vectors.")
    
    # Process using local PySpark contexts
    aggregated_results = spark_engine.calculate_batch_aggregations(records)
    
    # FIXED: Persist Spark execution calculations back to MongoDB collection store
    aggregated_results["calculated_at"] = datetime.datetime.utcnow()
    aggregated_results["queryKey"] = f"{assetId}::{dataSourceId}"
    dal.db.spark_analysis_cache.insert_one(aggregated_results)
    
    if "_id" in aggregated_results:
        aggregated_results["_id"] = str(aggregated_results["_id"])
        
    return aggregated_results

@app.get("/api/v1/analytics/predict", tags=["Analytics & Mining (Spark Powered)"])
def run_spark_ml_forecaster(assetId: str, dataSourceId: str):
    records = dal.get_time_series(assetId, dataSourceId)
    if not records:
        raise HTTPException(status_code=404, detail="No sequence logs found to train model pipelines.")
    
    prediction_results = spark_engine.execute_market_predictive_workflow(records)
    
    # FIXED: Persist Spark ML regression forecast state values directly to MongoDB cache
    if prediction_results.get("status") == "Success":
        prediction_results["persisted_at"] = datetime.datetime.utcnow()
        prediction_results["queryKey"] = f"{assetId}::{dataSourceId}"
        dal.db.spark_analysis_cache.insert_one(prediction_results)
        if "_id" in prediction_results:
            prediction_results["_id"] = str(prediction_results["_id"])
            
    return prediction_results
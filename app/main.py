import datetime
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from app.dal import DataAccessLayer
from app.spark_engine import SparkAnalyticsEngine

app = FastAPI(title="Acme Ltd Financial Data Warehouse Pro", version="2.0.0")
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

# --- UC 2: Refactored Consumption Enforcers (With Pagination!) ---

@app.get("/api/v1/assets", tags=["Data Consumption"])
def query_all_assets(
    limit: int = Query(10, ge=1, le=100, description="Pagination page size limit"),
    offset: int = Query(0, ge=0, description="Pagination offsets skipping index factor"),
    as_of: Optional[str] = None
):
    target_time = datetime.datetime.fromisoformat(as_of) if as_of else None
    return dal.get_all_assets(limit=limit, offset=offset, as_of=target_time)

@app.get("/api/v1/assets/{assetId}", tags=["Data Consumption"])
def query_asset_details(assetId: str, as_of: Optional[str] = None):
    target_time = datetime.datetime.fromisoformat(as_of) if as_of else None
    asset = dal.get_active_asset(assetId, as_of=target_time)
    if not asset:
        raise HTTPException(status_code=404, detail="Asset profile history slice variant not found.")
    return asset

@app.get("/api/v1/sources", tags=["Data Consumption"])
def query_all_data_sources(
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0)
):
    return dal.get_all_sources(limit=limit, offset=offset)

@app.get("/api/v1/sources/{dataSourceId}", tags=["Data Consumption"])
def query_data_source_details(dataSourceId: str):
    src = dal.get_data_source(dataSourceId)
    if not src:
        raise HTTPException(status_code=404, detail="Data vendor profile record not found.")
    return src

@app.get("/api/v1/timeseries", tags=["Data Consumption"])
def query_time_series(assetId: str, dataSourceId: str, start_date: Optional[str] = None, end_date: Optional[str] = None):
    start = datetime.datetime.fromisoformat(start_date) if start_date else None
    end = datetime.datetime.fromisoformat(end_date) if end_date else None
    return dal.get_time_series(assetId, dataSourceId, start, end)

# --- UC 3: Complete Core Apache Spark Core Analytics Layer ---

@app.get("/api/v1/analytics/aggregate", tags=["Analytics & Mining (Spark Powered)"])
def run_spark_aggregation(assetId: str, dataSourceId: str):
    records = dal.get_time_series(assetId, dataSourceId)
    if not records:
        raise HTTPException(status_code=404, detail="No historical logs found to feed Spark context.")
    return spark_engine.calculate_batch_aggregations(records)

@app.get("/api/v1/analytics/predict", tags=["Analytics & Mining (Spark Powered)"])
def run_spark_ml_forecaster(assetId: str, dataSourceId: str):
    records = dal.get_time_series(assetId, dataSourceId)
    if not records:
        raise HTTPException(status_code=404, detail="No historical records found to build forecasting vectors.")
    return spark_engine.execute_market_predictive_workflow(records)
import datetime
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from pymongo import MongoClient, DESCENDING

app = FastAPI(title="Acme Ltd Financial Data Warehouse", version="1.0.0")

# Local Native MongoDB Connection String
client = MongoClient("mongodb://localhost:27017/")
db = client["acme_financial_dwh"]

# --- Pydantic Data Schemas (Heterogeneous & Temporal Support) ---

class DataSourceSchema(BaseModel):
    dataSourceId: str
    name: str
    description: str
    api_endpoint: Optional[str] = None
    inserted_at: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)

class FinancialAssetSchema(BaseModel):
    assetId: str
    symbol: str
    asset_class: str  # stock, bond, crypto, etc.
    description: str
    region_origin: str
    valid_from: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    valid_to: Optional[datetime.datetime] = None  
    is_deleted: bool = False
    additional_attributes: Dict[str, Any] = Field(default_factory=dict) # Handles heterogeneous properties

class TimeSeriesRecordSchema(BaseModel):
    assetId: str
    dataSourceId: str
    timestamp: datetime.datetime
    indicators: Dict[str, Any]  # Dynamic metrics: open, close, volume, ask_size
    recorded_at: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)

# --- Temporal Helper Utilities ---

def get_active_asset(asset_id: str, as_of: Optional[datetime.datetime] = None) -> Optional[dict]:
    """Retrieves the active version for an asset at a specific point in time."""
    target_time = as_of or datetime.datetime.utcnow()
    query = {
        "assetId": asset_id,
        "valid_from": {"$lte": target_time},
        "$or": [
            {"valid_to": None},
            {"valid_to": {"$gt": target_time}}
        ]
    }
    asset = db.financial_assets.find_one(query, sort=[("valid_from", DESCENDING)])
    if asset and not asset.get("is_deleted", False):
        asset["_id"] = str(asset["_id"])
        return asset
    return None

# --- UC 1: Data Ingest & Provenance Engine ---

@app.post("/api/v1/sources", tags=["Ingestion"])
def register_data_source(source: DataSourceSchema):
    if db.data_sources.find_one({"dataSourceId": source.dataSourceId}):
        raise HTTPException(status_code=400, detail="Data Source ID already registered.")
    db.data_sources.insert_one(source.model_dump())
    return {"status": "Success", "message": f"Data Source {source.dataSourceId} saved."}

@app.post("/api/v1/assets", tags=["Ingestion"])
def upsert_financial_asset(asset_data: FinancialAssetSchema):
    """Temporal DB Pattern: Never overwrite. Create a new version instead."""
    now = datetime.datetime.utcnow()
    existing_active = get_active_asset(asset_data.assetId, as_of=now)
    
    if existing_active:
        # Close the validity window for the old version
        db.financial_assets.update_one(
            {"_id": existing_active["_id"]},
            {"$set": {"valid_to": now}}
        )
    
    asset_dict = asset_data.model_dump()
    asset_dict["valid_from"] = now
    asset_dict["valid_to"] = None
    db.financial_assets.insert_one(asset_dict)
    return {"status": "Success", "version_start": now}

@app.delete("/api/v1/assets/{assetId}", tags=["Ingestion"])
def soft_delete_financial_asset(assetId: str):
    """Temporal deletion: Inserts an explicit deletion marker record."""
    now = datetime.datetime.utcnow()
    existing_active = get_active_asset(assetId, as_of=now)
    
    if not existing_active:
        raise HTTPException(status_code=404, detail="Active Asset record not found.")
        
    db.financial_assets.update_one(
        {"_id": existing_active["_id"]},
        {"$set": {"valid_to": now}}
    )
    
    marker = {
        "assetId": assetId,
        "symbol": existing_active["symbol"],
        "asset_class": existing_active["asset_class"],
        "description": "DELETED MARKER",
        "region_origin": existing_active["region_origin"],
        "valid_from": now,
        "valid_to": None,
        "is_deleted": True,
        "additional_attributes": {}
    }
    db.financial_assets.insert_one(marker)
    return {"status": "Success", "message": f"Asset {assetId} temporal soft-delete recorded."}

@app.post("/api/v1/timeseries", tags=["Ingestion"])
def ingest_time_series(records: List[TimeSeriesRecordSchema]):
    for rec in records:
        if not db.data_sources.find_one({"dataSourceId": rec.dataSourceId}):
            raise HTTPException(status_code=400, detail=f"Data Source {rec.dataSourceId} invalid.")
    
    documents = [r.model_dump() for r in records]
    db.time_series.insert_many(documents)
    return {"status": "Success", "records_ingested": len(documents)}


# --- UC 2: Consumption REST API ---

@app.get("/api/v1/assets", tags=["Data Consumption"])
def query_all_assets(as_of: Optional[str] = None):
    """[Q1] Return limited identification data about all active assets."""
    target_time = datetime.datetime.fromisoformat(as_of) if as_of else datetime.datetime.utcnow()
    pipeline = [
        {"$match": {"valid_from": {"$lte": target_time}, "$or": [{"valid_to": None}, {"valid_to": {"$gt": target_time}}]}},
        {"$match": {"is_deleted": False}},
        {"$project": {"_id": 0, "assetId": 1, "symbol": 1, "asset_class": 1}}
    ]
    return list(db.financial_assets.aggregate(pipeline))

@app.get("/api/v1/assets/{assetId}", tags=["Data Consumption"])
def query_asset_details(assetId: str, as_of: Optional[str] = None):
    """[Q2] Return all details of an asset knowing its identifier."""
    target_time = datetime.datetime.fromisoformat(as_of) if as_of else datetime.datetime.utcnow()
    asset = get_active_asset(assetId, as_of=target_time)
    if not asset:
        raise HTTPException(status_code=404, detail="Asset metadata not found for this point in time.")
    return asset

@app.get("/api/v1/sources", tags=["Data Consumption"])
def query_all_data_sources():
    """[Q3] Return summary data about all data sources available."""
    return list(db.data_sources.find({}, {"_id": 0, "dataSourceId": 1, "name": 1}))

@app.get("/api/v1/sources/{dataSourceId}", tags=["Data Consumption"])
def query_data_source_details(dataSourceId: str):
    """[Q4] Return details of a data source knowing its identifier."""
    source = db.data_sources.find_one({"dataSourceId": dataSourceId}, {"_id": 0})
    if not source:
        raise HTTPException(status_code=404, detail="Data provider source entry not found.")
    return source

@app.get("/api/v1/timeseries", tags=["Data Consumption"])
def query_time_series_data(
    assetId: str = Query(...), 
    dataSourceId: str = Query(...),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """[Q5] Return time-series data for specified asset and data source identifiers."""
    query = {"assetId": assetId, "dataSourceId": dataSourceId}
    if start_date or end_date:
        query["timestamp"] = {}
        if start_date:
            query["timestamp"]["$gte"] = datetime.datetime.fromisoformat(start_date)
        if end_date:
            query["timestamp"]["$lte"] = datetime.datetime.fromisoformat(end_date)
            
    return list(db.time_series.find(query, {"_id": 0}).sort("timestamp", 1))


# --- UC 3: Analytical Engine Aggregations ---

@app.get("/api/v1/analytics/aggregate", tags=["Analytics & Mining"])
def extract_metrics_summary(assetId: str, metric_key: str = "close"):
    """Provides statistical metrics (min/max/avg) out of the data."""
    pipeline = [
        {"$match": {"assetId": assetId}},
        {"$group": {
            "_id": "$assetId",
            "total_records": {"$sum": 1},
            "minimum_value": {"$min": f"$indicators.{metric_key}"},
            "maximum_value": {"$max": f"$indicators.{metric_key}"},
            "average_value": {"$avg": f"$indicators.{metric_key}"}
        }}
    ]
    result = list(db.time_series.aggregate(pipeline))
    if not result:
        raise HTTPException(status_code=404, detail="No time-series data found for calculation.")
    return result[0]
import requests

# --- UC 1: External API Pulling & Provenance Engine ---

@app.post("/api/v1/ingest/external/crypto", tags=["Ingestion"])
def fetch_live_external_crypto_data(coin_id: str = "bitcoin"):
    """
    Connects directly to the live CoinGecko REST API, extracts real market metrics,
    and structures the data into the DWH with strict data provenance tracing.
    """
    # 1. Ensure the external vendor source profile is registered in our DWH tracking system
    provider_id = "COINGECKO_PUBLIC_API"
    if not db.data_sources.find_one({"dataSourceId": provider_id}):
        db.data_sources.insert_one({
            "dataSourceId": provider_id,
            "name": "CoinGecko API Platform",
            "description": "Public data vendor for global cryptocurrency market metrics."
        })
    
    # 2. Call the live external REST API endpoint
    external_url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd&include_24hr_vol=true"
    
    try:
        response = requests.get(external_url)
        data = response.json()
        
        if coin_id not in data:
            raise HTTPException(status_code=404, detail=f"Asset token '{coin_id}' not found on external provider server.")
            
        market_metrics = data[coin_id]
        now = datetime.datetime.utcnow()
        asset_id = f"{coin_id.upper()}_CRYPTO"
        
        # 3. Handle data provenance: ensure the asset asset profile is registered
        if not get_active_asset(asset_id, as_of=now):
            db.financial_assets.insert_one({
                "assetId": asset_id,
                "symbol": coin_id.upper()[:4],
                "asset_class": "crypto",
                "description": f"{coin_id.capitalize()} Digital Currency Asset",
                "region_origin": "Global",
                "valid_from": now,
                "valid_to": None,
                "is_deleted": False,
                "additional_attributes": {"network": "Blockchain Native"}
            })
            
        # 4. Save the time-series metric tracking document, recording explicit provenance details
        timeseries_document = {
            "assetId": asset_id,
            "dataSourceId": provider_id,  # <--- Traces exactly where this data point came from
            "timestamp": now,
            "indicators": {
                "close": market_metrics["usd"],
                "volume_24h": market_metrics["usd_24h_vol"]
            },
            "recorded_at": now
        }
        
        db.time_series.insert_one(timeseries_document)
        return {
            "status": "Success",
            "message": f"Successfully pulled live metrics for {coin_id} from external provider.",
            "provenance_logged": {
                "assetId": asset_id,
                "dataSourceId": provider_id,
                "extracted_indicators": timeseries_document["indicators"]
            }
        }
        
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"External data processing link crash error: {str(err)}")
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
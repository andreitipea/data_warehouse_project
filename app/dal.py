import datetime
from typing import Any, Dict, List, Optional
from pymongo import MongoClient, DESCENDING

class DataAccessLayer:
    def __init__(self, connection_string: str = "mongodb://localhost:27017/"):
        self.client = MongoClient(connection_string)
        self.db = self.client["acme_financial_dwh"]
        
    # --- Temporal & Versioning Operations ---
    
    def get_active_asset(self, asset_id: str, as_of: Optional[datetime.datetime] = None) -> Optional[dict]:
        """[Deterministic Ordering] Locates active asset profile state at any historical checkpoint."""
        target_time = as_of or datetime.datetime.utcnow()
        query = {
            "assetId": asset_id,
            "valid_from": {"$lte": target_time},
            "$or": [
                {"valid_to": None},
                {"valid_to": {"$gt": target_time}}
            ]
        }
        asset = self.db.financial_assets.find_one(query, sort=[("valid_from", DESCENDING)])
        if asset and not asset.get("is_deleted", False):
            asset["_id"] = str(asset["_id"])
            return asset
        return None

    def save_asset(self, asset_data: dict) -> dict:
        """[Pure Temporal Append-Only] Closes historical intervals using immutable version inserts."""
        now = datetime.datetime.utcnow()
        asset_id = asset_data["assetId"]
        
        # Pull existing history to handle version closeout without mutating fields in-place
        existing_active = self.get_active_asset(asset_id, as_of=now)
        if existing_active:
            # Enforce true immutability: insert a specific closure tracking record instead of using update_one
            closure_record = existing_active.copy()
            del closure_record["_id"]
            closure_record["valid_to"] = now
            self.db.financial_assets.insert_one(closure_record)
            
        # Insert your active version tracking branch record
        asset_data["valid_from"] = now
        asset_data["valid_to"] = None
        asset_data["is_deleted"] = False
        self.db.financial_assets.insert_one(asset_data)
        return {"status": "Success", "version_start": now}

    def soft_delete_asset(self, asset_id: str) -> bool:
        """Inserts an explicit marker tracking payload to indicate asset deletion."""
        now = datetime.datetime.utcnow()
        existing_active = self.get_active_asset(asset_id, as_of=now)
        if not existing_active:
            return False
            
        # Create an immutable version closure copy
        closure_record = existing_active.copy()
        del closure_record["_id"]
        closure_record["valid_to"] = now
        self.db.financial_assets.insert_one(closure_record)
        
        # Append our explicit deletion marker record
        marker = {
            "assetId": asset_id,
            "symbol": existing_active["symbol"],
            "asset_class": existing_active["asset_class"],
            "description": "DELETED MARKER",
            "region_origin": existing_active["region_origin"],
            "valid_from": now,
            "valid_to": None,
            "is_deleted": True,
            "additional_attributes": {}
        }
        self.db.financial_assets.insert_one(marker)
        return True

    # --- Ingestion Verification Logics ---
    
    def save_data_source(self, source_data: dict) -> bool:
        """Saves a registered financial vendor source profile."""
        if self.db.data_sources.find_one({"dataSourceId": source_data["dataSourceId"]}):
            return False
        self.db.data_sources.insert_one(source_data)
        return True

    def get_data_source(self, source_id: str) -> Optional[dict]:
        return self.db.data_sources.find_one({"dataSourceId": source_id}, {"_id": 0})

    def get_all_sources(self, limit: int = 10, offset: int = 0) -> List[dict]:
        """Provides paginated access to recorded ingestion metadata headers."""
        return list(self.db.data_sources.find({}, {"_id": 0, "dataSourceId": 1, "name": 1}).skip(offset).limit(limit))

    def get_all_assets(self, limit: int = 10, offset: int = 0, as_of: Optional[datetime.datetime] = None) -> List[dict]:
        """[Paginated Request Handling] Pulls dynamic list variants safely matching time frames."""
        target_time = as_of or datetime.datetime.utcnow()
        pipeline = [
            {"$match": {"valid_from": {"$lte": target_time}, "$or": [{"valid_to": None}, {"valid_to": {"$gt": target_time}}]}},
            {"$match": {"is_deleted": False}},
            {"$skip": offset},
            {"$limit": limit},
            {"$project": {"_id": 0, "assetId": 1, "symbol": 1, "asset_class": 1}}
        ]
        return list(self.db.financial_assets.aggregate(pipeline))

    def save_time_series_batch(self, records: List[dict]) -> int:
        """Inserts chronological tick indices straight to the repository database."""
        for record in records:
            if not self.get_data_source(record["dataSourceId"]):
                raise ValueError(f"Data source reference {record['dataSourceId']} invalid.")
        result = self.db.time_series.insert_many(records)
        return len(result.inserted_ids)

    def get_time_series(self, asset_id: str, source_id: str, start: Optional[datetime.datetime] = None, end: Optional[datetime.datetime] = None) -> List[dict]:
        query = {"assetId": asset_id, "dataSourceId": source_id}
        if start or end:
            query["timestamp"] = {}
            if start: query["timestamp"]["$gte"] = start
            if end: query["timestamp"]["$lte"] = end
        return list(self.db.time_series.find(query, {"_id": 0}).sort("timestamp", 1))
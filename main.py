import asyncio
import json
from typing import Set, Dict, List, Any
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Body
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    DateTime,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select, update, delete
from datetime import datetime
from pydantic import BaseModel, field_validator
from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

# FastAPI app setup
app = FastAPI()
# SQLAlchemy setup
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
# Define the ProcessedAgentData table
processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("user_id", Integer),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)
SessionLocal = sessionmaker(bind=engine)


# SQLAlchemy model
class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    user_id: int
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime


# FastAPI models
class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float


class GpsData(BaseModel):
    latitude: float
    longitude: float


class AgentData(BaseModel):
    user_id: int
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @classmethod
    @field_validator("timestamp", mode="before")
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)."
            )


class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData


# WebSocket subscriptions
subscriptions: Dict[int, Set[WebSocket]] = {}


# FastAPI WebSocket endpoint
@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await websocket.accept()
    if user_id not in subscriptions:
        subscriptions[user_id] = set()
    subscriptions[user_id].add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions[user_id].remove(websocket)


# Function to send data to subscribed users
async def send_data_to_subscribers(user_id: int, data):
    if user_id in subscriptions:
        for websocket in subscriptions[user_id]:
            await websocket.send_json(json.dumps(data))


# FastAPI CRUDL endpoints
def prepare_insert_query(record):
    """Підготовка запиту для вставки даних."""
    return processed_agent_data.insert().values(
        road_state=record.road_state,
        user_id=record.agent_data.user_id,
        x=record.agent_data.accelerometer.x,
        y=record.agent_data.accelerometer.y,
        z=record.agent_data.accelerometer.z,
        latitude=record.agent_data.gps.latitude,
        longitude=record.agent_data.gps.longitude,
        timestamp=record.agent_data.timestamp
    )

@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: List[ProcessedAgentData]):
    db_session = SessionLocal()

    try:
        for record in data:
            insert_query = prepare_insert_query(record)

            insert_result = db_session.execute(insert_query)
            db_session.commit()
            
            await send_data_to_subscribers(record.agent_data.user_id, insert_result)
    except Exception as exc:
        db_session.rollback()
        raise exc
    finally:
        db_session.close()

@app.get("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def read_processed_agent_data(processed_agent_data_id: int):
    db_session = SessionLocal()
    try:
        select_query = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        selected_item = db_session.execute(select_query).fetchone()
        if selected_item is None:
            raise HTTPException(status_code=404, detail="Error, input data not found")

        return ProcessedAgentDataInDB(**selected_item._asdict())
    finally:
        db_session.close()



@app.get("/processed_agent_data/", response_model=list[ProcessedAgentDataInDB])
def list_processed_agent_data():
    db_session = SessionLocal()
    try:
        select_all_query = select(processed_agent_data)
        all_results = db_session.execute(select_all_query).fetchall()
        return [ProcessedAgentDataInDB(**row._asdict()) for row in all_results]
    finally:
        db_session.close()


def execute_update_query(db_session, processed_agent_data_id: int, data: ProcessedAgentData):

    update_query = update(processed_agent_data).where(
        processed_agent_data.c.id == processed_agent_data_id
    ).values(
        road_state=data.road_state,
        user_id=data.agent_data.user_id,
        x=data.agent_data.accelerometer.x,
        y=data.agent_data.accelerometer.y,
        z=data.agent_data.accelerometer.z,
        latitude=data.agent_data.gps.latitude,
        longitude=data.agent_data.gps.longitude,
        timestamp=data.agent_data.timestamp
    )

    update_result = db_session.execute(update_query)
    db_session.commit()
    return update_result

@app.put("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData):
    db_session = SessionLocal()
    try:
        update_result = execute_update_query(db_session, processed_agent_data_id, data)
        
        if update_result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Error, input data not found")
        
        refreshed_query = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        refreshed_item = db_session.execute(refreshed_query).fetchone()

        if refreshed_item is None:
            raise HTTPException(status_code=404, detail="Failed to fetch updated ProcessedAgentData")

        return ProcessedAgentDataInDB(**refreshed_item._asdict())
    finally:
        db_session.close()


@app.delete("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def delete_processed_agent_data(processed_agent_data_id: int):
    db_session = SessionLocal()
    try:
        select_before_delete_query = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        item_to_delete = db_session.execute(select_before_delete_query).fetchone()
        if item_to_delete is None:
            raise HTTPException(status_code=404, detail="Error, input data not found")
        delete_query = delete(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        db_session.execute(delete_query)
        db_session.commit()
        return ProcessedAgentDataInDB(**item_to_delete._asdict())
    finally:
        db_session.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
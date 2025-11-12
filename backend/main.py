import asyncio
import json
from datetime import datetime
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from contextlib import asynccontextmanager
from pydantic import BaseModel
from models import F1_DRIVERS
from kafka_producer import position_producer
from kafka_consumer import kafka_consumer
from race_manager import race_manager
from config import config

class RaceStartRequest(BaseModel):
    driver_name: str

# Global storage for SSE connections
position_connections = set()
commentary_connections = set()
anomaly_connections = set()

# Event loop for async operations from threads
_loop = None

def get_event_loop():
    """Get or create the event loop for async operations from threads"""
    global _loop
    if _loop is None:
        try:
            _loop = asyncio.get_event_loop()
        except RuntimeError:
            _loop = asyncio.new_event_loop()
            asyncio.set_event_loop(_loop)
    return _loop

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("Starting F1 Leaderboard API...")
    
    # Store event loop for use in callbacks
    global _loop
    _loop = asyncio.get_event_loop()
    
    # Start Kafka consumer
    kafka_consumer.start_consuming()
    
    # Add callbacks for SSE updates
    kafka_consumer.add_position_callback(notify_position_connections)
    kafka_consumer.add_commentary_callback(notify_commentary_connections)
    
    # Add anomaly callback if feature is enabled
    if config.is_anomaly_detection_enabled():
        kafka_consumer.add_anomaly_callback(notify_anomaly_connections)
    
    # Set up race manager with position producer
    race_manager.position_producer = position_producer
    
    # Start cleanup task for finished races
    asyncio.create_task(race_manager.start_cleanup_task())
    
    yield
    
    # Shutdown
    print("Shutting down F1 Leaderboard API...")
    position_producer.stop()
    kafka_consumer.stop_consuming()
    race_manager.stop_cleanup_task()

app = FastAPI(title="F1 Leaderboard API", lifespan=lifespan)

# CORS middleware for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],  # React dev servers
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def notify_position_connections(positions, race_id=None):
    """Notify all connected clients about position updates"""
    try:
        global position_connections
        if not position_connections:
            return
        
        data = {
            "type": "positions",
            "data": {
                "positions": positions,  # positions is already a list of dicts
                "timestamp": datetime.now().isoformat(),
                "race_id": race_id
            }
        }
        
        message = f"data: {json.dumps(data)}\n\n"
        
        # Send to all connected clients
        disconnected = []
        for connection in list(position_connections):
            try:
                connection.put_nowait(message)
            except:
                disconnected.append(connection)
        
        # Remove disconnected clients
        for connection in disconnected:
            position_connections.discard(connection)
    except Exception as e:
        print(f"Error in notify_position_connections: {e}")

def notify_commentary_connections(commentary):
    """Notify all connected clients about commentary updates - optimized for minimum latency"""
    try:
        global commentary_connections, _loop
        if not commentary_connections:
            return
        
        # Prepare message once (more efficient)
        data = {
            "type": "commentary",
            "data": {
                "id": commentary.id,
                "message": commentary.message,
                "timestamp": commentary.timestamp.isoformat(),
                "type": commentary.type
            }
        }
        
        message = f"data: {json.dumps(data)}\n\n"
        
        # Schedule notification in event loop for immediate processing
        # This ensures the notification happens asynchronously without blocking
        if _loop and _loop.is_running():
            # Use call_soon_threadsafe to schedule from consumer thread
            _loop.call_soon_threadsafe(_send_commentary_message, message)
        else:
            # Fallback to direct send if loop not available
            _send_commentary_message(message)
    except Exception as e:
        print(f"Error in notify_commentary_connections: {e}")

def _send_commentary_message(message):
    """Actually send the commentary message to all connections"""
    global commentary_connections
    connections_list = list(commentary_connections)
    for connection in connections_list:
        try:
            # Use put_nowait for immediate non-blocking send
            connection.put_nowait(message)
        except asyncio.QueueFull:
            # Queue full - skip this connection (shouldn't happen with proper sizing)
            pass
        except Exception:
            # Connection is closed or invalid - remove it
            commentary_connections.discard(connection)

def notify_anomaly_connections(anomaly):
    """Notify all connected clients about anomaly updates"""
    try:
        global anomaly_connections, _loop
        if not anomaly_connections:
            return
        
        # Prepare message once
        data = {
            "type": "anomaly",
            "data": {
                "team_name": anomaly.get('team_name', ''),
                "driver_name": anomaly.get('driver_name', ''),
                "race_id": anomaly.get('race_id', ''),
                "timestamp": anomaly.get('timestamp', int(datetime.now().timestamp() * 1000)),
                "metric_name": anomaly.get('metric_name', ''),
                "metric_value": anomaly.get('metric_value', 0.0),
                "confidence_score": anomaly.get('confidence_score', 0.0),
                "severity": anomaly.get('severity', 'info')
            }
        }
        
        message = f"data: {json.dumps(data)}\n\n"
        
        # Schedule notification in event loop for immediate processing
        if _loop and _loop.is_running():
            _loop.call_soon_threadsafe(_send_anomaly_message, message)
        else:
            _send_anomaly_message(message)
    except Exception as e:
        print(f"Error in notify_anomaly_connections: {e}")

def _send_anomaly_message(message):
    """Actually send the anomaly message to all connections"""
    global anomaly_connections
    connections_list = list(anomaly_connections)
    for connection in connections_list:
        try:
            connection.put_nowait(message)
        except asyncio.QueueFull:
            pass
        except Exception:
            anomaly_connections.discard(connection)

@app.get("/")
async def root():
    return {"message": "F1 Leaderboard API is running"}

@app.get("/api/drivers")
async def get_drivers():
    """Get list of all F1 drivers"""
    return {"drivers": [driver.dict() for driver in F1_DRIVERS]}

@app.post("/api/race/start")
async def start_race(request: RaceStartRequest):
    """Start a new race for the specified driver"""
    try:
        # Create race
        race_id = race_manager.create_race(request.driver_name)
        
        # Start race simulation in background
        asyncio.create_task(race_manager.start_race_simulation(race_id))
        
        return {
            "race_id": race_id,
            "driver_name": request.driver_name,
            "status": "started",
            "message": f"Race started for {request.driver_name}"
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start race: {str(e)}")

@app.get("/api/race/{race_id}/status")
async def get_race_status(race_id: str):
    """Get status of a specific race"""
    status = race_manager.get_race_status(race_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Race not found")
    return status

@app.get("/api/race/{race_id}/positions")
async def get_race_positions(race_id: str):
    """Get final positions for a finished race"""
    positions = race_manager.get_final_positions(race_id)
    if positions is None:
        raise HTTPException(status_code=404, detail="Race not found or not finished")
    return {"positions": positions}

@app.get("/api/positions/stream")
async def stream_positions(race_id: str = Query(None, description="Optional race ID to filter positions")):
    """SSE endpoint for real-time position updates"""
    async def event_generator():
        # Create a queue for this connection
        queue = asyncio.Queue()
        position_connections.add(queue)
        
        try:
            # Send initial positions if available
            latest_positions = kafka_consumer.get_latest_positions(race_id)
            if latest_positions:
                data = {
                    "type": "positions",
                    "data": {
                        "positions": latest_positions,  # positions is already a list of dicts
                        "timestamp": datetime.now().isoformat(),
                        "race_id": race_id
                    }
                }
                yield f"data: {json.dumps(data)}\n\n"
            
            # Keep connection alive and send updates
            while True:
                try:
                    # Wait for new data with timeout
                    message = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield message
                except asyncio.TimeoutError:
                    # Send keepalive
                    yield f"data: {json.dumps({'type': 'ping'})}\n\n"
        
        finally:
            # Remove connection when client disconnects
            position_connections.discard(queue)
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )

@app.get("/api/commentary/stream")
async def stream_commentary():
    """SSE endpoint for real-time commentary updates - optimized for minimum latency"""
    async def event_generator():
        # Create a queue for this connection with larger size for high throughput
        queue = asyncio.Queue(maxsize=1000)
        commentary_connections.add(queue)
        
        try:
            # Send initial commentary if available (skip to reduce initial delay)
            # latest_commentary = kafka_consumer.get_latest_commentary()
            # if latest_commentary:
            #     for commentary in latest_commentary[-10:]:  # Send last 10 entries
            #         data = {
            #             "type": "commentary",
            #             "data": {
            #                 "id": commentary.id,
            #                 "message": commentary.message,
            #                 "timestamp": commentary.timestamp.isoformat(),
            #                 "type": commentary.type
            #             }
            #         }
            #         yield f"data: {json.dumps(data)}\n\n"
            
            # Keep connection alive and send updates immediately
            while True:
                try:
                    # Use very short timeout (0.5s) to check for messages frequently
                    # This reduces latency significantly
                    message = await asyncio.wait_for(queue.get(), timeout=0.5)
                    yield message
                except asyncio.TimeoutError:
                    # Send keepalive less frequently
                    yield f"data: {json.dumps({'type': 'ping'})}\n\n"
        
        finally:
            # Remove connection when client disconnects
            commentary_connections.discard(queue)
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )

@app.get("/api/anomalies/stream")
async def stream_anomalies(race_id: str = Query(None, description="Optional race ID to filter anomalies")):
    """SSE endpoint for real-time anomaly updates"""
    # Check if feature is enabled
    if not config.is_anomaly_detection_enabled():
        raise HTTPException(status_code=404, detail="Anomaly detection feature is not enabled")
    
    async def event_generator():
        queue = asyncio.Queue(maxsize=1000)
        anomaly_connections.add(queue)
        
        try:
            # Send initial anomalies if available
            latest_anomalies = kafka_consumer.get_latest_anomalies(race_id)
            if latest_anomalies:
                for anomaly in latest_anomalies[-20:]:  # Send last 20 entries
                    data = {
                        "type": "anomaly",
                        "data": {
                            "team_name": anomaly.get('team_name', ''),
                            "driver_name": anomaly.get('driver_name', ''),
                            "race_id": anomaly.get('race_id', ''),
                            "timestamp": anomaly.get('timestamp', int(datetime.now().timestamp() * 1000)),
                            "metric_name": anomaly.get('metric_name', ''),
                            "metric_value": anomaly.get('metric_value', 0.0),
                            "confidence_score": anomaly.get('confidence_score', 0.0),
                            "severity": anomaly.get('severity', 'info')
                        }
                    }
                    yield f"data: {json.dumps(data)}\n\n"
            
            # Keep connection alive and send updates
            while True:
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=0.5)
                    yield message
                except asyncio.TimeoutError:
                    yield f"data: {json.dumps({'type': 'ping'})}\n\n"
        
        finally:
            anomaly_connections.discard(queue)
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )

@app.get("/api/anomalies/latest")
async def get_latest_anomalies(race_id: str = Query(None, description="Optional race ID to filter anomalies")):
    """Get latest anomalies"""
    # Check if feature is enabled
    if not config.is_anomaly_detection_enabled():
        raise HTTPException(status_code=404, detail="Anomaly detection feature is not enabled")
    
    anomalies = kafka_consumer.get_latest_anomalies(race_id)
    return {"anomalies": anomalies[-50:]}  # Return last 50 anomalies

@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "active_connections": {
            "positions": len(position_connections),
            "commentary": len(commentary_connections),
            "anomalies": len(anomaly_connections) if config.is_anomaly_detection_enabled() else 0
        },
        "features": {
            "anomaly_detection": config.is_anomaly_detection_enabled()
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)

# api/main.py
# FastAPI application entry point.
# All routes are registered here.

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from database import verify_connection
from api.routes import physicians, leads


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Runs on startup
    print("Starting Physician Lead System API...")
    verify_connection()
    yield
    # Runs on shutdown
    print("Shutting down API...")


app = FastAPI(
    title="Physician Lead System API",
    description=(
        "Backend API for physician lead generation. "
        "Provides filtered, scored physician records "
        "for AI-powered outreach campaigns."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allows frontend or calling system to reach this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten this in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(
    physicians.router,
    prefix="/api/v1/physicians",
    tags=["Physicians"]
)
app.include_router(
    leads.router,
    prefix="/api/v1/leads",
    tags=["Leads"]
)


@app.get("/", tags=["Health"])
def root():
    return {
        "system": "Physician Lead System",
        "version": "1.0.0",
        "status": "running",
    }


@app.get("/health", tags=["Health"])
def health_check():
    db_ok = verify_connection()
    return {
        "status": "healthy" if db_ok else "degraded",
        "database": "connected" if db_ok else "disconnected",
    }
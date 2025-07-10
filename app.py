from faker import Faker
import pandas as pd
import random
import datetime
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from typing import List, Union, Optional
import io
from synthetic_data_generator import generate_synthetic_data
from fastapi.middleware.cors import CORSMiddleware
import os


app = FastAPI(title="Synthetic Data Generator API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount the static directory
app.mount("/static", StaticFiles(directory="static"), name="static")

# Serve index.html at the root URL
@app.get("/")
async def read_root():
    return FileResponse('static/index.html')


# Pydantic models for request validation
class ColumnConfig(BaseModel):
    column_name: str
    data_type: str
    category_values: Optional[List[str]] = None
    range_start: Optional[Union[int, float]] = None
    range_end: Optional[Union[int, float]] = None

class DataGeneratorRequest(BaseModel):
    columns_config: List[ColumnConfig]
    num_rows: int = Field(gt=0, le=1000000, description="Number of rows to generate (max 1,000,000)")


@app.post("/generate-data")
async def generate_data(request: DataGeneratorRequest):
    """
    Generate synthetic data based on the provided configuration and return as CSV
    
    Args:
        request (DataGeneratorRequest): Configuration for data generation
        
    Returns:
        StreamingResponse: CSV file containing the generated data
    """
    try:
        # Convert Pydantic models to dictionaries
        columns_config = [column.model_dump(exclude_none=True) for column in request.columns_config]
        
        # Generate the data
        df = generate_synthetic_data(columns_config, request.num_rows)
        
        # Create a buffer to store the CSV
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        
        # Return the CSV file as a downloadable response
        return StreamingResponse(
            iter([buffer.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=synthetic_data_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            }
        )
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

# Example usage and documentation
@app.get("/")
async def root():
    """
    Return example usage and documentation
    """
    return {
        "message": "Welcome to the Synthetic Data Generator API",
        "example_request": {
            "columns_config": [
                {
                    "column_name": "full_name",
                    "data_type": "name"
                },
                {
                    "column_name": "email",
                    "data_type": "email"
                },
                {
                    "column_name": "status",
                    "data_type": "categorized",
                    "category_values": ["Active", "Inactive", "Pending"]
                },
                {
                    "column_name": "score",
                    "data_type": "range",
                    "range_start": 0,
                    "range_end": 100
                }
            ],
            "num_rows": 100
        },
        "available_data_types": [
            "name", "email", "phone", "address", "company", "job_title",
            "date", "number", "boolean", "text", "url", "username",
            "ip_address", "credit_card", "country", "city",
            "categorized", "range"
        ]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
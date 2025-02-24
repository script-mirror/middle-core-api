from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class ProductCreate(BaseModel):
    name: str
    base_url: str
    date_pattern: str
    date_difference_unit: str
    function_name: str
    updated_at: Optional[datetime]

class ProductRead(BaseModel):
    id: int
    name: str
    base_url: str
    date_pattern: str
    date_difference_unit: str
    function_name: str
    updated_at: Optional[datetime]
    date_differences: List['DateDifferenceRead']
    executions: List['ExecutionRead']

    class Config:
        from_attributes = True

class DateDifferenceCreate(BaseModel):
    format_order: int
    product_id: int
    date_difference: int

class DateDifferenceRead(BaseModel):
    id: int
    format_order: int
    product_id: int
    date_difference: int

    class Config:
        from_attributes = True
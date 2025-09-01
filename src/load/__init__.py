"""Load specific processes"""

from .dimensions import (
    dim_age,
    dim_country,
    dim_date,
    dim_region,
    dim_source,
    dim_status,
    dim_vaccine,
    maintain_eu_dims,
    maintain_shared_dims,
    maintain_uk_dims,
)
from .dw import create_dimensional_model_eu, create_schema, drop_dimensional_model_eu
from .facts import maintain_eu_facts, maintain_uk_facts
from .load import load_all, load_eu, load_uk

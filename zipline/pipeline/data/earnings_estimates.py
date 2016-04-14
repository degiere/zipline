"""
Datasets representing earnings estimates data.
"""
from zipline.utils.numpy_utils import datetime64ns_dtype, float64_dtype

from .dataset import Column, DataSet


class EarningsEstimates(DataSet):
    """
    Dataset representing earnings estimates data.
    """
    previous_release_date = Column(datetime64ns_dtype)
    next_release_date = Column(datetime64ns_dtype)
    previous_standard_deviation = Column(float64_dtype)
    next_standard_deviation = Column(float64_dtype)
    previous_count = Column(float64_dtype)
    next_count = Column(float64_dtype)
    previous_fiscal_quarter = Column(float64_dtype)
    next_fiscal_quarter = Column(float64_dtype)
    previous_high = Column(float64_dtype)
    next_high = Column(float64_dtype)
    previous_mean = Column(float64_dtype)
    next_mean = Column(float64_dtype)
    previous_fiscal_year = Column(float64_dtype)
    next_fiscal_year = Column(float64_dtype)
    previous_low = Column(float64_dtype)
    next_low = Column(float64_dtype)

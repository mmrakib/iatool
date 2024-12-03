from typing import Union

import pandas as pd

def get_date_range(data: Union[pd.Series, pd.DataFrame], start_date: str, end_date: str) -> Union[pd.Series, pd.DataFrame]:
    if not isinstance(data.index, pd.DatetimeIndex):
        raise ValueError("Data must have a datetime index")
    
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    filtered_data = data[(data.index >= start_date) & (data.index <= end_date)]

    return filtered_data

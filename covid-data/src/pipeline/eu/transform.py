import pandas as pd

def movement_indicators(file_path):
    data = pd.read_json(file_path)

    print(data.columns)
    return data
import pandas as pd

def process_data(file_path, queue):
    df = pd.read_csv(file_path, low_memory=False)
    queue.put(df.to_dict())

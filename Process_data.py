import pandas as pd
from PyQt6 import QtWidgets, uic, QtCore
#def process_data(file_path, queue):
#    df = pd.read_csv(file_path, low_memory=False)
#    queue.put(df.to_dict())
def process_chunk(chunk):
    cleaned = chunk.dropna()
    return cleaned
def read_in_chunks(file_path, chunk_size=10000):
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        yield chunk
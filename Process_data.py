import pandas as pd
from PyQt6 import QtWidgets, uic, QtCore
def process_chunk(chunk):
    cleaned = chunk.dropna()
    return cleaned
def read_in_chunks(file_path, chunk_size=10000):
    for chunk in pd.read_csv(file_path, chunksize=chunk_size, low_memory=False):
        yield chunk

def count_rows(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        for i, _ in enumerate(f):
            pass
    return i
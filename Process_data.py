import pandas as pd

def read_in_chunks(file_path, chunk_size=10000):
    for chunk in pd.read_csv(file_path, chunksize=chunk_size, low_memory=False):
        yield chunk

def count_rows(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        for i, _ in enumerate(f):
            pass
    return i


def process_chunk(chunk_df):
    """
    Funkcja przetwarzająca pojedynczy chunk danych i obliczająca częściowe statystyki.
    Zwraca słownik z sumami/licznikami dla tego chunka.
    """
    # WAŻNE: Dostosuj to do Twoich KOLUMN i logiki czyszczenia
    # Usuwamy wiersze z wartościami NaN w kolumnach kluczowych
    required_columns = ['fare_amount', 'tip_amount', 'payment_type', 'airport_fee']
    existing_required_columns = [col for col in required_columns if col in chunk_df.columns]

    if not existing_required_columns:
        # Jeśli brakuje kluczowych kolumn, zwracamy puste statystyki dla tego chunka
        # W rzeczywistości, możesz chcieć logować błąd lub inaczej obsługiwać.
        return {
            'num_trips': 0,
            'sum_fare_amount': 0.0,
            'sum_tip_amount': 0.0,
            'card_payments': 0,
            'cash_payments': 0,
            'airport_fees_count': 0
        }

    # Wykonaj oczyszczanie danych na chunku (np. usunięcie NaN, filtracja)
    # To jest kluczowe dla jakości danych. Dopasuj do swoich potrzeb.
    cleaned_chunk = chunk_df.dropna(subset=existing_required_columns)

    # Przykładowa dodatkowa filtracja:
    if 'trip_distance' in cleaned_chunk.columns:
        cleaned_chunk = cleaned_chunk[cleaned_chunk['trip_distance'] > 0]

    # Oblicz częściowe statystyki dla tego ODCZYSZCZONEGO chunka
    num_trips = len(cleaned_chunk)
    sum_fare_amount = cleaned_chunk['fare_amount'].sum() if 'fare_amount' in cleaned_chunk.columns else 0.0
    sum_tip_amount = cleaned_chunk['tip_amount'].sum() if 'tip_amount' in cleaned_chunk.columns else 0.0

    card_payments = (cleaned_chunk["payment_type"] == 1).sum() if 'payment_type' in cleaned_chunk.columns else 0
    cash_payments = (cleaned_chunk["payment_type"] == 2).sum() if 'payment_type' in cleaned_chunk.columns else 0

    airport_fees_count = (cleaned_chunk["airport_fee"] > 0).sum() if 'airport_fee' in cleaned_chunk.columns else 0

    return {
        'num_trips': num_trips,
        'sum_fare_amount': sum_fare_amount,
        'sum_tip_amount': sum_tip_amount,
        'card_payments': card_payments,
        'cash_payments': cash_payments,
        'airport_fees_count': airport_fees_count
    }

# Usuń stare funkcje calculate_num_trips, calculate_avg_fare_amount itd.
# Nie będą już potrzebne, bo statystyki są obliczane w process_chunk
# Jeśli masz zdefiniowane 'calculate_all_stats', też ją usuń
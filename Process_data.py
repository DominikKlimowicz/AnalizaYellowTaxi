from datetime import datetime
import os

import pandas as pd

def read_in_chunks(file_path, chunk_size=1000000):
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

    suspicious_trips_in_chunk = []

    if 'tip_amount' in cleaned_chunk.columns and \
            'fare_amount' in cleaned_chunk.columns and \
            'payment_type' in cleaned_chunk.columns:

        card_payments_df = cleaned_chunk[cleaned_chunk['payment_type'] == 1]

        # Upewnij się, że fare_amount jest większe od 0
        card_payments_df = card_payments_df[card_payments_df['fare_amount'] > 0]

        # Warunek dla "dużych" kwot (np. powyżej 40 PLN)
        min_fare_amount_for_suspicious = 40


        suspicious_high_tips = card_payments_df[
            (card_payments_df['tip_amount'] >= card_payments_df['fare_amount'] * 2) &
            (card_payments_df['fare_amount'] >= min_fare_amount_for_suspicious)
            ]

        # Konwertuj znalezione wiersze na listę słowników, aby łatwo je przekazać
        if not suspicious_high_tips.empty:
            suspicious_trips_in_chunk.extend(
                suspicious_high_tips[['VendorID','trip_distance', 'fare_amount', 'tip_amount', 'payment_type']].to_dict('records')
            )

    # Przykładowa dodatkowa filtracja:
    if 'trip_distance' in cleaned_chunk.columns:
        cleaned_chunk = cleaned_chunk[cleaned_chunk['trip_distance'] > 0]

    # Oblicz częściowe statystyki dla tego ODCZYSZCZONEGO chunka
    num_trips = len(cleaned_chunk)
    sum_fare_amount = cleaned_chunk['fare_amount'].sum() if 'fare_amount' in cleaned_chunk.columns else 0.0

    # ZMIANA: Przenieś filtrowanie card_payments_df na górę, bo jest używane do sum_tip_amount
    card_payments_df = cleaned_chunk[
        cleaned_chunk['payment_type'] == 1] if 'payment_type' in cleaned_chunk.columns else pd.DataFrame()
    sum_tip_amount = card_payments_df['tip_amount'].sum() if 'tip_amount' in card_payments_df.columns else 0.0

    card_payments = (cleaned_chunk["payment_type"] == 1).sum() if 'payment_type' in cleaned_chunk.columns else 0
    cash_payments = (cleaned_chunk["payment_type"] == 2).sum() if 'payment_type' in cleaned_chunk.columns else 0

    airport_fees_count = (cleaned_chunk["airport_fee"] > 0).sum() if 'airport_fee' in cleaned_chunk.columns else 0

    return {
        'num_trips': num_trips,
        'sum_fare_amount': sum_fare_amount,
        'sum_tip_amount': sum_tip_amount,
        'card_payments': card_payments,
        'cash_payments': cash_payments,
        'airport_fees_count': airport_fees_count,
        'suspicious_trips': suspicious_trips_in_chunk  # Musi być zawsze zwracane
    }

def generate_report(stats_data, original_file_path=None):
    """
    Generuje plik tekstowy z raportem zawierającym obliczone statystyki.
    Plik będzie miał nazwę Raport_X.txt, gdzie X to kolejny numer.

    Args:
        stats_data (dict): Słownik zawierający obliczone statystyki.
                           Powinien mieć klucze takie jak 'LiczbaKursow', 'SrOplata', itp.
    Returns:
        str: Ścieżka do utworzonego pliku raportu.
    """
    report_directory = "Raporty"
    if not os.path.exists(report_directory):
        try:
            os.makedirs(report_directory)
            print(f"Utworzono folder: {report_directory}")
        except OSError as e:
            print(f"Błąd podczas tworzenia folderu {report_directory}: {e}")
            return None

    report_number = 1
    report_filename_base = f"Raport_{report_number}.txt"
    report_full_path = os.path.join(report_directory, report_filename_base)
    while os.path.exists(report_full_path):
        report_number += 1
        report_filename_base = f"Raport_{report_number}.txt"
        report_full_path = os.path.join(report_directory, report_filename_base)

    try:
        with open(report_full_path, 'w', encoding='utf-8') as f:
            f.write("--- RAPORT ANALIZY DANYCH TAXI ---\n")
            f.write(f"Data wygenerowania: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            if original_file_path:
                file_name = os.path.basename(original_file_path)
                f.write(f"Plik źródłowy: {file_name}\n")
            f.write("----------------------------------\n\n")

            f.write(f"Liczba wszystkich kursów: {stats_data.get('LiczbaKursow', 'N/A')}\n")
            f.write(f"Średnia opłata za przejazd: {stats_data.get('SrOplata', 0.0):.2f} USD\n")
            f.write(f"Średnia kwota napiwku (tylko karty): {stats_data.get('SrNapiwek', 0.0):.2f} USD\n")
            f.write(f"Liczba płatności kartą: {stats_data.get('LKarta', 'N/A')}\n")
            f.write(f"Liczba płatności gotówką: {stats_data.get('LGotowka', 'N/A')}\n")
            f.write(f"Liczba kursów z opłatą lotniskową: {stats_data.get('IloscLotnisk', 'N/A')}\n")
            suspicious_trips = stats_data.get('PodejrzanePrzejazdy', [])
            if suspicious_trips:
                f.write("\n--- PODEJRZANE PRZEJAZDY (Napiwek >= 200% kwoty przejazdu i kwota przejazdu > 40 USD) ---\n")
                for i, trip in enumerate(suspicious_trips):
                    f.write(
                        f"  {i + 1}. VendorID: {trip.get('VendorID', 'N/A')}, Dystans: {trip.get('trip_distance', 0.0):.2f} mil, "
                        f"Kwota przejazdu: {trip.get('fare_amount', 0.0):.2f} USD, Napiwek: {trip.get('tip_amount', 0.0):.2f} USD, "
                        f"Typ płatności: {trip.get('payment_type', 'N/A')}\n"
                    )
                f.write(f"  Łącznie znaleziono: {len(suspicious_trips)} podejrzanych przejazdów.\n")
            else:
                f.write("\n--- Brak podejrzanych przejazdów spełniających kryteria ---\n")

            f.write("\n--- Koniec Raportu ---\n")
        print(f"Raport zapisano do pliku: {report_full_path}")
        return report_full_path
    except IOError as e:
        print(f"Błąd podczas zapisywania raportu do pliku {report_full_path}: {e}")
        return None

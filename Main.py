import multiprocessing
import sys

from PyQt6 import uic
from PyQt6 import QtCore, QtWidgets
from PyQt6.QtWidgets import QFileDialog, QMessageBox, QApplication
from multiprocessing import set_start_method, Pool
from Process_data import *
import pandas as pd

class Worker(QtCore.QThread):
    progress_update = QtCore.pyqtSignal(int)
    # Zmieniamy sygnał, żeby zwracał TYLKO słownik statystyk, bo nie tworzymy już całego DF
    finished_processing = QtCore.pyqtSignal(dict)

    def __init__(self, file_path):
        super().__init__()
        self.file_path = file_path

    def run(self):
        # --- ETAP 1: Wczytywanie, czyszczenie i zbieranie statystyk z chunków (równolegle) ---
        try:
            total_rows = count_rows(self.file_path)
        except Exception as e:
            print(f"Błąd podczas zliczania wierszy: {e}. Używam szacowanej liczby chunków.")
            total_rows = 1  # Domyślna wartość

        chunk_size = 10000
        total_chunks = (total_rows // chunk_size) + (1 if total_rows % chunk_size else 0)
        if total_chunks == 0 and total_rows > 0:
            total_chunks = 1
        elif total_chunks == 0 and total_rows == 0:
            total_chunks = 1

        chunk_generator = read_in_chunks(self.file_path, chunk_size=chunk_size)

        # Inicjalizacja słownika do sumowania statystyk
        total_stats = {
            'num_trips': 0,
            'sum_fare_amount': 0.0,
            'sum_tip_amount': 0.0,
            'card_payments': 0,
            'cash_payments': 0,
            'airport_fees_count': 0
        }

        num_cores = multiprocessing.cpu_count()

        print(f"Rozpoczynam przetwarzanie chunków i agregację statystyk w {num_cores} procesach...")

        with Pool(processes=num_cores) as pool:
            # pool.imap leniwie wywołuje process_chunk dla każdego chunka
            for i, chunk_stats in enumerate(pool.imap(process_chunk, chunk_generator), start=1):
                # Sumujemy statystyki z każdego chunka
                total_stats['num_trips'] += chunk_stats['num_trips']
                total_stats['sum_fare_amount'] += chunk_stats['sum_fare_amount']
                total_stats['sum_tip_amount'] += chunk_stats['sum_tip_amount']
                total_stats['card_payments'] += chunk_stats['card_payments']
                total_stats['cash_payments'] += chunk_stats['cash_payments']
                total_stats['airport_fees_count'] += chunk_stats['airport_fees_count']

                # Aktualizacja paska postępu
                if total_chunks > 0:
                    progress_percent = int(i / total_chunks * 100)
                else:
                    progress_percent = 100
                self.progress_update.emit(progress_percent)

        print("\nPrzetwarzanie chunków i zbieranie statystyk zakończone.")

        # Oblicz średnie na koniec (tylko jeśli num_trips > 0, aby uniknąć dzielenia przez zero)
        final_stats = {
            'LiczbaKursow': total_stats['num_trips'],
            'SrOplata': total_stats['sum_fare_amount'] / total_stats['num_trips'] if total_stats[
                                                                                         'num_trips'] > 0 else 0.0,
            'SrNapiwek': total_stats['sum_tip_amount'] / total_stats['num_trips'] if total_stats[
                                                                                         'num_trips'] > 0 else 0.0,
            'LKarta': total_stats['card_payments'],
            'LGotowka': total_stats['cash_payments'],
            'IloscLotnisk': total_stats['airport_fees_count']
        }

        # Zakończenie: Emitujemy tylko final_stats
        self.finished_processing.emit(final_stats)


class TaxiApp(QtWidgets.QDialog):
    def __init__(self):
        super().__init__()
        try:
            uic.loadUi("QT_GUI/glowneOkno.ui", self)
        except Exception as e:
            QMessageBox.critical(self, "Błąd wczytywania UI",
                                 f"Nie można załadować pliku UI: QT_GUI/glowneOkno.ui\nBłąd: {e}")
            sys.exit(1)

        self.PrzyciskWczytaj.clicked.connect(self.on_button_clicked)
        self.worker = None
        self.PasekLadowania.setValue(0)

    def on_button_clicked(self):
        file_dialog = QFileDialog(self)
        file_dialog.setFileMode(QFileDialog.FileMode.ExistingFile)
        file_dialog.setNameFilter("Pliki CSV (*.csv)")
        self.PasekLadowania.setValue(0)

        if file_dialog.exec():
            selected_file = file_dialog.selectedFiles()[0]

            # Wymagany import os do walidacji
            import os  # Dodaj to na początku Main.py, jeśli go tam nie ma
            if not selected_file or not os.path.exists(selected_file) or os.path.getsize(selected_file) == 0:
                QMessageBox.warning(self, "Błąd pliku", "Wybrany plik jest pusty lub nie istnieje.")
                return

            self.worker = Worker(selected_file)
            self.worker.progress_update.connect(self.PasekLadowania.setValue)
            # Teraz finished_processing emituje tylko słownik stats
            self.worker.finished_processing.connect(self.on_processing_finished)
            self.worker.start()
        else:
            QMessageBox.information(self, "Anulowano", "Wczytywanie pliku anulowano.")

    # Zmieniamy sygnaturę funkcji, aby przyjmowała tylko stats
    def on_processing_finished(self, stats):
        # Przekazujemy tylko stats do nowego okna
        # Usunęliśmy df z argumentów, bo już go nie przekazujemy
        self.analiza_window = OknoAnalizy(stats)
        self.analiza_window.show()
        self.close()

class OknoAnalizy(QtWidgets.QDialog):
    # Zmieniamy konstruktor, aby przyjmował tylko stats
    def __init__(self, stats):
        super().__init__()
        try:
            uic.loadUi("QT_GUI/OknoAnalizy.ui", self)
        except Exception as e:
            QMessageBox.critical(self, "Błąd wczytywania UI", f"Nie można załadować pliku UI: QT_GUI/OknoAnalizy.ui\nBłąd: {e}")
            sys.exit(1)

        # Przechowujemy statystyki
        self.stats = stats

        # Ustawiamy wartości w GUI z użyciem słownika stats
        # Pamiętaj, że klucze w final_stats w Workerze są takie same jak nazwy pól GUI
        self.LiczbaKursow.setText(str(self.stats['LiczbaKursow']))
        self.SrOplata.setText(f"{self.stats['SrOplata']:.2f}")
        self.SrNapiwek.setText(f"{self.stats['SrNapiwek']:.2f}")
        self.LKarta.setText(str(self.stats['LKarta']))
        self.LGotowka.setText(str(self.stats['LGotowka']))
        self.IloscLotnisk.setText(str(self.stats['IloscLotnisk']))

        self.PrzyciskZamknij.clicked.connect(QApplication.quit)
if __name__ == "__main__":
    set_start_method("spawn")
    app = QtWidgets.QApplication(sys.argv)
    dialog = TaxiApp()
    dialog.show()
    sys.exit(app.exec())

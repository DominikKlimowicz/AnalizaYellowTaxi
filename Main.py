import multiprocessing
import sys

from PyQt6 import uic
from PyQt6 import QtCore, QtWidgets
from PyQt6.QtWidgets import QFileDialog, QMessageBox, QApplication
from multiprocessing import set_start_method, Pool
from Process_data import *

class Worker(QtCore.QThread):
    progress_update = QtCore.pyqtSignal(int)
    finished_processing = QtCore.pyqtSignal(dict)

    def __init__(self, file_path):
        super().__init__()
        self.file_path = file_path

    def run(self):
        try:
            total_rows = count_rows(self.file_path)
        except Exception as e:
            print(f"Błąd podczas zliczania wierszy: {e}. Używam szacowanej liczby chunków.")
            total_rows = 1

        chunk_size = 10000
        total_chunks = (total_rows // chunk_size) + (1 if total_rows % chunk_size else 0)
        if total_chunks == 0 and total_rows > 0:
            total_chunks = 1
        elif total_chunks == 0 and total_rows == 0:
            total_chunks = 1

        chunk_generator = read_in_chunks(self.file_path, chunk_size=chunk_size)

        total_stats = {
            'num_trips': 0,
            'sum_fare_amount': 0.0,
            'sum_tip_amount': 0.0,
            'card_payments': 0,
            'cash_payments': 0,
            'airport_fees_count': 0,
        }

        all_suspicious_trips = []

        num_cores = multiprocessing.cpu_count()

        print(f"Rozpoczynam przetwarzanie chunków i agregację statystyk w {num_cores} procesach...")

        with Pool(processes=num_cores) as pool:
            for i, chunk_stats in enumerate(pool.imap(process_chunk, chunk_generator), start=1):
                total_stats['num_trips'] += chunk_stats['num_trips']
                total_stats['sum_fare_amount'] += chunk_stats['sum_fare_amount']
                total_stats['sum_tip_amount'] += chunk_stats['sum_tip_amount']
                total_stats['card_payments'] += chunk_stats['card_payments']
                total_stats['cash_payments'] += chunk_stats['cash_payments']
                total_stats['airport_fees_count'] += chunk_stats['airport_fees_count']
                all_suspicious_trips.extend(chunk_stats.get('suspicious_trips', []))

                if total_chunks > 0:
                    progress_percent = int(i / total_chunks * 100)
                else:
                    progress_percent = 100
                self.progress_update.emit(progress_percent)

        print("\nPrzetwarzanie chunków i zbieranie statystyk zakończone.")

        final_stats = {
            'LiczbaKursow': total_stats['num_trips'],
            'SrOplata': total_stats['sum_fare_amount'] / total_stats['num_trips'] if total_stats[
                                                                                         'num_trips'] > 0 else 0.0,
            'SrNapiwek': total_stats['sum_tip_amount'] / total_stats['num_trips'] if total_stats[
                                                                                         'num_trips'] > 0 else 0.0,
            'LKarta': total_stats['card_payments'],
            'LGotowka': total_stats['cash_payments'],
            'IloscLotnisk': total_stats['airport_fees_count'],
            'PodejrzanePrzejazdy': all_suspicious_trips
        }

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

            if not selected_file or not os.path.exists(selected_file) or os.path.getsize(selected_file) == 0:
                QMessageBox.warning(self, "Błąd pliku", "Wybrany plik jest pusty lub nie istnieje.")
                return

            self.file_path = selected_file
            self.worker = Worker(selected_file)
            self.worker.progress_update.connect(self.PasekLadowania.setValue)
            self.worker.finished_processing.connect(self.on_processing_finished)
            self.worker.start()
        else:
            QMessageBox.information(self, "Anulowano", "Wczytywanie pliku anulowano.")

    def on_processing_finished(self, stats):
        QMessageBox.information(self, "Zakończono", "Przetwarzanie danych zakończone. Generowanie raportu.")

        report_path = generate_report(stats, self.file_path)
        if report_path:
            QMessageBox.information(self, "Raport", f"Raport wygenerowany: {report_path}")
        else:
            QMessageBox.warning(self, "Błąd Raportu", "Nie udało się wygenerować raportu.")
        self.analiza_window = OknoAnalizy(stats)
        self.analiza_window.show()
        self.close()

class OknoAnalizy(QtWidgets.QDialog):
    def __init__(self, stats):
        super().__init__()
        try:
            uic.loadUi("QT_GUI/OknoAnalizy.ui", self)
        except Exception as e:
            QMessageBox.critical(self, "Błąd wczytywania UI", f"Nie można załadować pliku UI: QT_GUI/OknoAnalizy.ui\nBłąd: {e}")
            sys.exit(1)

        self.stats = stats

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
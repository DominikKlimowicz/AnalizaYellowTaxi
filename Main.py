import sys
from PyQt6 import QtWidgets, uic, QtCore
from PyQt6.QtWidgets import QFileDialog, QMessageBox
import pandas as pd
from multiprocessing import Process, set_start_method, Pool
from Process_data import *

class Worker(QtCore.QThread):
    progress_update = QtCore.pyqtSignal(int)  # sygnał do GUI, żeby aktualizować pasek
    finished_processing = QtCore.pyqtSignal(pd.DataFrame)  # sygnał z gotowymi danymi

    def __init__(self, file_path):
        super().__init__()
        self.file_path = file_path

    def run(self):
        chunks = list(read_in_chunks(self.file_path))
        total_chunks = len(chunks)
        cleaned_chunks = []

        with Pool(processes=4) as pool:
            for i, result in enumerate(pool.imap(process_chunk, chunks), 1):
                cleaned_chunks.append(result)
                progress_percent = int((i / total_chunks) * 100)
                self.progress_update.emit(progress_percent)

        cleaned_df = pd.concat(cleaned_chunks)
        self.finished_processing.emit(cleaned_df)

class TaxiApp(QtWidgets.QDialog):
    def __init__(self):
        super().__init__()
        uic.loadUi("QT_GUI/glowneOkno.ui", self)

        # Podpięcie przycisku
        self.PrzyciskWczytaj.clicked.connect(self.on_button_clicked)
        self.worker = None

    def on_button_clicked(self):
        # Otwórz okno dialogowe do wyboru pliku
        file_dialog = QFileDialog(self)
        file_dialog.setFileMode(QFileDialog.FileMode.ExistingFiles)
        file_dialog.setNameFilter("Pliki CSV (*.csv)")
        if file_dialog.exec():
            selected_file = file_dialog.selectedFiles()[0]

            self.worker = Worker(selected_file)
            self.worker.progress_update.connect(self.PasekLadowania.setValue)
            self.worker.finished_processing.connect(self.on_processing_finished)
            self.worker.start()

    def on_processing_finished(self, df):
        QMessageBox.information(self, "Zakończono", f"Przetworzono {len(df)} wierszy bez pustych wartości.")



    def update_progress_bar(self):
        if self.progress_value < 100:
            self.progress_value += 10
            self.PasekLadowania.setValue(self.progress_value)
        else:
            self.timer.stop()



if __name__ == "__main__":
    set_start_method("spawn")
    app = QtWidgets.QApplication(sys.argv)
    dialog = TaxiApp()
    dialog.show()
    sys.exit(app.exec())
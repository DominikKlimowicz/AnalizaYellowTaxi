import sys
from PyQt6 import QtWidgets, uic, QtCore
from PyQt6.QtWidgets import QFileDialog, QMessageBox
import pandas as pd


class TaxiApp(QtWidgets.QDialog):
    def __init__(self):
        super().__init__()
        uic.loadUi("QT_GUI/glowneOkno.ui", self)

        # Podpięcie przycisku
        self.PrzyciskWczytaj.clicked.connect(self.on_button_clicked)

        self.timer = QtCore.QTimer(self)
        self.timer.timeout.connect(self.update_progress_bar)
        self.progress_value = 0

    def on_button_clicked(self):
        # Otwórz okno dialogowe do wyboru pliku
        file_dialog = QFileDialog(self)
        file_dialog.setFileMode(QFileDialog.FileMode.ExistingFiles)
        file_dialog.setNameFilter("Pliki CSV (*.csv)")
        if file_dialog.exec():
            selected_file = file_dialog.selectedFiles()[0]
            self.show_file_preview(selected_file)

            # Uruchom pasek postępu
            self.progress_value = 0
            self.PasekLadowania.setValue(0)
            self.timer.start(1000)

    def show_file_preview(self, file_path):
        try:
            # Wczytaj dane z pliku CSV
            data = pd.read_csv(file_path)
            # Pokaż pierwsze 2 linijki w MessageBox
            preview = data.head(2).to_string(index=False)
            QMessageBox.information(self, "Podgląd pliku", f"Pierwsze 2 linie:\n{preview}")
        except Exception as e:
            QMessageBox.warning(self, "Błąd", f"Nie udało się wczytać pliku: {str(e)}")

    def update_progress_bar(self):
        if self.progress_value < 100:
            self.progress_value += 10
            self.PasekLadowania.setValue(self.progress_value)
        else:
            self.timer.stop()


if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    dialog = TaxiApp()
    dialog.show()
    sys.exit(app.exec())

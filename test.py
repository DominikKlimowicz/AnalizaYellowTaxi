import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QMessageBox
from QT_GUI.ui_glowneOkno import Ui_QDialog

class TaxiApp(QMainWindow, Ui_MainWindow):
    def __init__(self):
        super().__init__()
        self.setupUi(self)

        # Zakładam, że masz przycisk o nazwie 'loadButton' i pasek 'progressBar'
        self.loadButton.clicked.connect(self.on_button_clicked)
        self.progress_value = 0

        # Timer do symulacji ładowania
        self.timer = self.startTimer(1000)  # 1 sekunda

    def timerEvent(self, event):
        if self.progress_value < 100:
            self.progress_value += 10
            self.progressBar.setValue(self.progress_value)
        else:
            self.killTimer(event.timerId())

    def on_button_clicked(self):
        QMessageBox.information(self, "Kliknięto", "Załadowano dane!")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    okno = TaxiApp()
    okno.show()
    sys.exit(app.exec_())

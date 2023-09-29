import logging
from PyQt5.QtWidgets import QMainWindow, QDesktopWidget, QWidget
from PyQt5.QtCore import QPropertyAnimation
from .design import Ui_MainWindow
from .logic import Download_Tables_Logic, SugestaoLogic, BuscaLogic

logger = logging.getLogger(__name__)


class MainWindowLogic(QMainWindow, Ui_MainWindow):

    def __init__(self):
        super().__init__()
        self.setupUi(self)
        self.view.setCurrentIndex(0)

        self.download_tables_logic = Download_Tables_Logic(self)
        self.sugestao_logic = SugestaoLogic(self)
        self.search_logic = BuscaLogic(self)

        #  Adjust the geometry based on the screen size
        screen = QDesktopWidget().screenGeometry()
        width, height = screen.width(), screen.height()
        self.setGeometry(0, 0, int(width * 0.9), int(height * 0.9))

        self.home_button.clicked.connect(lambda: self.switch_view(0))
        self.relatorios_button.clicked.connect(lambda: self.switch_view(1))
        self.sug_comp_button.clicked.connect((lambda: self.switch_view(2)))
        self.sug_import_button.clicked.connect((lambda: self.switch_view(3)))
        self.search_button.clicked.connect((lambda: self.switch_view(4)))

        # set progress bar min and max to keep it stopped by default
        self.progressBar.setMinimum(0)
        self.progressBar.setMaximum(100)
        self.progress_sug.setMinimum(0)
        self.progress_sug.setMaximum(100)

    def switch_view(self, index):
        self.view.setCurrentIndex(index)

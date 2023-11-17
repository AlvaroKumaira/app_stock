import logging
import os
from datetime import datetime
from PyQt5.QtWidgets import QMainWindow, QDesktopWidget, QWidget, QFileDialog
from PyQt5.QtCore import QPropertyAnimation, Qt, QPoint
from .design import Ui_MainWindow
from .logic import Download_Tables_Logic, SugestaoLogic, BuscaLogic, Analysis_Report_Logic, Table_Search_Logic
from .download_thread import DownloadThread
from database_functions.params_update import save_excel_locally
from main_functions.sugestao_compra import create_final_df

logger = logging.getLogger(__name__)


class MainWindowLogic(QMainWindow, Ui_MainWindow):

    def __init__(self):
        super().__init__()
        self.create_df_thread = None
        self.update_excel_thread = None
        self.setupUi(self)
        self.view.setCurrentIndex(0)
        self.setWindowFlags(Qt.FramelessWindowHint)
        self._dragging = False
        self._drag_position = QPoint()
        self.start_download_threads()
        self.download_tables_logic = Download_Tables_Logic(self)
        self.sugestao_logic = SugestaoLogic(self)
        self.search_logic = BuscaLogic(self)
        self.report_logic = Analysis_Report_Logic(self)
        self.table_search_logic = Table_Search_Logic(self)

        #  Adjust the geometry based on the screen size
        screen = QDesktopWidget().screenGeometry()
        width, height = screen.width(), screen.height()
        self.adjustSize()

        # Define functions to button clicks (switch view)
        self.close_button.clicked.connect(self.close)
        self.minimize_button.clicked.connect(self.showMinimized)
        self.home_button.clicked.connect(lambda: self.switch_view(0))
        self.relatorios_button.clicked.connect(lambda: self.switch_view(1))
        self.relatorios_button_2.clicked.connect(lambda: self.switch_view(2))
        self.table_button.clicked.connect(lambda: self.switch_view(3))
        self.sug_comp_button.clicked.connect((lambda: self.switch_view(4)))
        self.sug_import_button.clicked.connect((lambda: self.switch_view(5)))
        self.search_button.clicked.connect((lambda: self.switch_view(6)))

        # Hide progress bars
        self.progressBar.hide()
        self.progress_sug.hide()
        self.progressBar_search.hide()
        self.table_progressbar.hide()
        self.progressBar_2.hide()

        # Define the drag logic
        self.utility_frame.mousePressEvent = self.utility_frame_mousePressEvent
        self.utility_frame.mouseMoveEvent = self.utility_frame_mouseMoveEvent
        self.utility_frame.mouseReleaseEvent = self.utility_frame_mouseReleaseEvent

    def switch_view(self, index):
        self.view.setCurrentIndex(index)

    def utility_frame_mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self._dragging = True
            self._drag_position = event.globalPos() - self.frameGeometry().topLeft()
            event.accept()

    def utility_frame_mouseMoveEvent(self, event):
        if self._dragging and event.buttons() == Qt.LeftButton:
            self.move(event.globalPos() - self._drag_position)
            event.accept()

    def utility_frame_mouseReleaseEvent(self, event):
        self._dragging = False
        event.accept()

    def has_update_occurred_today(self):
        last_run_file_path = os.path.join("params", "last_run_date.txt")
        if os.path.exists(last_run_file_path):
            with open(last_run_file_path, 'r') as f:
                last_run_date = f.read().strip()
            today_date = datetime.now().strftime('%Y-%m-%d')
            return last_run_date == today_date
        else:
            return False

    def record_update_occurrence(self):
        last_run_file_path = os.path.join("params", "last_run_date.txt")
        today_date = datetime.now().strftime('%Y-%m-%d')
        with open(last_run_file_path, 'w') as f:
            f.write(today_date)

    def start_download_threads(self):
        if not self.has_update_occurred_today():
            # Start the thread for updating the Excel file
            self.update_excel_thread = DownloadThread(
                save_excel_locally,
                "Dados_Sug.xlsx",
                shared_folder_path="Z:\\09 - Pecas\\Sgc"
            )
            self.update_excel_thread.progress_started.connect(self.on_progress_started)
            self.update_excel_thread.start()

            # Start the thread for creating the stock suggestion file
            self.create_df_thread = DownloadThread(
                create_final_df,
                '0101',
                False
            )
            self.create_df_thread.finished_with_result.connect(self.on_create_df_finished)
            self.create_df_thread.progress_started.connect(self.on_progress_started)
            self.create_df_thread.progress_stopped.connect(self.on_progress_stopped)
            self.create_df_thread.start()
        else:
            self.startup_bar.hide()
            logger.error("Application update has already been processed today")

    def on_create_df_finished(self, result):
        # Handle the result of the df creation
        save_excel_locally("Base_df.xlsx", data=result)

    def on_progress_started(self):
        # Show a loading indicator
        self.startup_bar.show()

    def on_progress_stopped(self):
        # Hide the loading indicator
        self.startup_bar.hide()
        self.record_update_occurrence()

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
from main_functions.analise_inventario import create_report

logger = logging.getLogger(__name__)


class MainWindowLogic(QMainWindow, Ui_MainWindow):

    def __init__(self):
        super().__init__()
        self.create_df_thread = None
        self.update_excel_thread = None
        self.update_inv_thread = None
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

        self.progressBar.hide()
        self.progress_sug.hide()
        self.progressBar_search.hide()
        self.progressBar_2.hide()
        self.startup_bar.hide()

        # Define a dictionary mapping buttons to view indexes
        button_to_view = {
            self.home_button: 0,
            self.relatorios_button: 1,
            self.relatorios_button_2: 2,
            self.sug_comp_button: 3,
            self.search_button: 4
        }

        # Connect each button to the switch_view function
        for button, view_index in button_to_view.items():
            button.clicked.connect(lambda _, v=view_index: self.switch_view(v))

        # Connect other buttons normally
        self.close_button.clicked.connect(self.close)
        self.minimize_button.clicked.connect(self.showMinimized)

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
            # Start the thread for updating the Excel files
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
                'Todas',
                False
            )
            self.create_df_thread.finished_with_result.connect(self.on_create_df_finished)
            self.create_df_thread.progress_started.connect(self.on_progress_started)
            self.create_df_thread.start()

            # Start the thred for updating the inventory file
            self.update_inv_thread = DownloadThread(
                create_report,
                'Todas', '12 meses', False
            )
            self.update_inv_thread.finished_with_result.connect(self.on_update_inv_finished)
            self.update_inv_thread.progress_started.connect(self.on_progress_started)
            self.update_inv_thread.progress_stopped.connect(self.on_progress_stopped)
        else:
            self.startup_bar.hide()
            logger.error("Application update has already been processed today")

    def on_create_df_finished(self, result):
        # Handle the result of the df creation
        save_excel_locally("Base_df.xlsx", data=result)

        self.update_inv_thread.start()

    def on_update_inv_finished(self, result):
        save_excel_locally("inv_df.xlsx", data=result)

    def on_progress_started(self):
        # Show a loading indicator
        self.progressBar.show()
        self.progress_sug.show()
        self.progressBar_search.show()
        self.progressBar_2.show()
        self.startup_bar.show()

    def on_progress_stopped(self):
        # Hide the loading indicator
        self.progressBar.hide()
        self.progress_sug.hide()
        self.progressBar_search.hide()
        self.progressBar_2.hide()
        self.startup_bar.hide()
        self.record_update_occurrence()

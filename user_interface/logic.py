import logging
import pandas
from . import resources_rc
from PyQt5.QtGui import QColor
from PyQt5.QtWidgets import QTableWidgetItem
from main_functions.download_tabelas import download_tabelas
from .download_thread import DownloadThread
from main_functions.sugestao_compra import create_final_df
from main_functions.busca_produtos import search_function

logger = logging.getLogger(__name__)


class BaseLogic:
    def __init__(self, ui):
        self.ui = ui
        self.download_thread = None

    def on_thread_finished(self):
        self.download_thread.deleteLater()

    def start_progress(self):
        self.ui.progressBar.setMinimum(0)
        self.ui.progressBar.setMaximum(0)
        self.ui.progress_sug.setMinimum(0)
        self.ui.progress_sug.setMaximum(0)

    def stop_progress(self):
        self.ui.progressBar.setMinimum(0)
        self.ui.progressBar.setMaximum(100)
        self.ui.progress_sug.setMinimum(0)
        self.ui.progress_sug.setMaximum(100)


class Download_Tables_Logic(BaseLogic):
    def __init__(self, ui):
        super().__init__(ui)
        self.setup_connections()

    def setup_connections(self):
        self.ui.download_button.clicked.connect(self.start_download_data)

    def start_download_data(self):
        saldo = self.ui.saldos_check.isChecked()
        pedidos = self.ui.pedidos_check.isChecked()
        faturamento = self.ui.faturamento_check.isChecked()
        filial = self.ui.filial_download.currentText()

        pedidos_selected_date = self.ui.pedidos_date_label.date().toPyDate()
        faturamento_selected_date = self.ui.faturamento_date_label.date().toPyDate()

        self.download_thread = DownloadThread(download_tabelas, filial, saldo, pedidos, faturamento,
                                              pedidos_selected_date, faturamento_selected_date)
        self.download_thread.progress_started.connect(self.start_progress)
        self.download_thread.progress_stopped.connect(self.stop_progress)
        self.download_thread.finished.connect(self.on_thread_finished)
        self.download_thread.start()


class SugestaoLogic(BaseLogic):
    def __init__(self, ui):
        super().__init__(ui)
        self.setup_connections()

    def setup_connections(self):
        self.ui.download_sug.clicked.connect(self.start_download_sug)

    def start_download_sug(self):
        filial = self.ui.filial_select.currentText()

        self.download_thread = DownloadThread(create_final_df, filial)
        self.download_thread.progress_started.connect(self.start_progress)
        self.download_thread.progress_stopped.connect(self.stop_progress)
        self.download_thread.finished_with_result.connect(self.display_sugestao_dataframe)
        self.download_thread.start()

    def display_sugestao_dataframe(self, df):
        """
        Display the dataframe in the tableWidget QTableWidget.
        """
        # Set the row count to the number of rows in the DataFrame
        self.ui.tableWidget.setRowCount(df.shape[0])

        # Create a QColor object for white
        white_color = QColor(255, 255, 255)

        # Populate the QTableWidget
        for row in range(df.shape[0]):
            for col in range(df.shape[1]):
                item = QTableWidgetItem(str(df.iat[row, col]))
                item.setForeground(white_color)  # Set the font color to white
                self.ui.tableWidget.setItem(row, col, item)


class BuscaLogic(BaseLogic):

    def __init__(self, ui):
        super().__init__(ui)
        self.setup_connections()

    def setup_connections(self):
        self.ui.search_start.clicked.connect(self.start_search)

    def start_search(self):
        product_id = self.ui.lineEdit.text()
        self.download_thread = DownloadThread(search_function, product_id)

        # Connect both update_labels and display_dataframe to the finished_with_result signal
        self.download_thread.finished_with_result.connect(self.update_labels)
        self.download_thread.finished_with_result.connect(self.display_dataframe)

        self.download_thread.start()

    def update_labels(self, df):
        group_value = df.iloc[0]['B1_ZGRUPO']
        desc_value = df.iloc[0]['B1_DESC']
        self.ui.agrup.setText(f"Agrupamento: {group_value}")
        self.ui.desc.setText(f"Descrição: {desc_value}")
        df.drop(columns=['B1_ZGRUPO', 'B1_DESC'], inplace=True)

    def display_dataframe(self, df):
        """
        Display the dataframe in the QTableWidget.
        """
        column_names = {"B1_COD": "Código", "B2_QATU": "Quantidade"}
        df.rename(columns=column_names, inplace=True)

        # Set the row count to the number of rows in the DataFrame
        self.ui.search_result.setRowCount(df.shape[0])

        # Create a QColor object for white
        white_color = QColor(255, 255, 255)

        # Populate the QTableWidget
        for row in range(df.shape[0]):
            for col in range(df.shape[1]):
                item = QTableWidgetItem(str(df.iat[row, col]))
                item.setForeground(white_color)  # Set the font color to white
                self.ui.search_result.setItem(row, col, item)

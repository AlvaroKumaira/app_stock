import logging
import pandas
from . import resources_rc
from PyQt5.QtGui import QColor
from PyQt5.QtWidgets import QTableWidgetItem, QCheckBox, QVBoxLayout
from main_functions.download_tabelas import download_tabelas
from .download_thread import DownloadThread
from main_functions.sugestao_compra import create_final_df
from main_functions.busca_produtos import search_function
from main_functions.analise_inventario import create_report
from main_functions.busca_tabelas import get_table_columns, download_save_table

logger = logging.getLogger(__name__)


class BaseLogic:
    def __init__(self, ui):
        self.ui = ui
        self.download_thread = None

    def on_thread_finished(self):
        self.download_thread.deleteLater()

    def start_progress(self):
        self.ui.progressBar.show()
        self.ui.progress_sug.show()
        self.ui.progressBar_search.show()
        self.ui.progressBar_2.show()

    def stop_progress(self):
        self.ui.progressBar.hide()
        self.ui.progress_sug.hide()
        self.ui.progressBar_search.hide()
        self.ui.progressBar_2.hide()


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

        self.download_thread = DownloadThread(create_final_df, filial, True)
        self.download_thread.progress_started.connect(self.start_progress)
        self.download_thread.progress_stopped.connect(self.stop_progress)
        self.download_thread.start()


class BuscaLogic(BaseLogic):

    def __init__(self, ui):
        super().__init__(ui)
        self.setup_connections()

    def setup_connections(self):
        self.ui.search_start.clicked.connect(self.start_search)

    def start_search(self):
        product_id = self.ui.lineEdit.text().strip()
        self.download_thread = DownloadThread(search_function, product_id)
        self.download_thread.progress_started.connect(self.start_progress)
        self.download_thread.progress_stopped.connect(self.stop_progress)

        # Connect both update_labels to the finished_with_result signal
        self.download_thread.finished_with_result.connect(self.update_labels)

        self.download_thread.start()

    def update_labels(self, df):
        def get_value_by_filial(df, column_name, filial_code, is_int=True):

            filtered_df = df[df['Filial'] == filial_code]

            if not filtered_df.empty:
                # Get the first item in the specified column
                value = filtered_df[column_name].iloc[0]

                if is_int:
                    try:
                        # Attempt to convert the value to an integer only if is_int is True
                        value = int(value)
                    except ValueError:
                        print(f"Conversion to int failed, keeping original value: {value}")
                return value
            else:
                return 0 if is_int else 'NE'

        if df is None or df.empty:
            self.clear_labels()
            self.ui.agrup_label.setText(f"Agrupamento: Não encontrado!")
            self.ui.desc_label.setText(f"Descrição: Não encontrado!")
            self.display_dataframe(df)
            return
        try:
            self.clear_labels()
            # Get all values from the table
            group_value = df.iloc[0]['Agrupamento']
            item_group_value = df.iloc[0]['Grupo']
            group_desc = df.iloc[0]['DescricaoGrupo']
            desc_value = df.iloc[0]['Descrição']
            min_value_m = get_value_by_filial(df, 'min', '0101')
            min_value_ca = get_value_by_filial(df, 'min', '0104')
            min_value_po = get_value_by_filial(df, 'min', '0103')
            min_value_pa = get_value_by_filial(df, 'min', '0105')

            max_value_m = get_value_by_filial(df, 'max', '0101')
            max_value_ca = get_value_by_filial(df, 'max', '0104')
            max_value_po = get_value_by_filial(df, 'max', '0103')
            max_value_pa = get_value_by_filial(df, 'max', '0105')

            seg_value_m = get_value_by_filial(df, 'Segurança', '0101')
            seg_value_ca = get_value_by_filial(df, 'Segurança', '0104')
            seg_value_po = get_value_by_filial(df, 'Segurança', '0103')
            seg_value_pa = get_value_by_filial(df, 'Segurança', '0105')

            nota_value_m = get_value_by_filial(df, 'Nota', '0101')
            nota_value_ca = get_value_by_filial(df, 'Nota', '0104')
            nota_value_po = get_value_by_filial(df, 'Nota', '0103')
            nota_value_pa = get_value_by_filial(df, 'Nota', '0105')

            call_value_m = get_value_by_filial(df, 'Vendas no período', '0101')
            call_value_ca = get_value_by_filial(df, 'Vendas no período', '0104')
            call_value_po = get_value_by_filial(df, 'Vendas no período', '0103')
            call_value_pa = get_value_by_filial(df, 'Vendas no período', '0105')

            dem_value_m = get_value_by_filial(df, 'Demanda no período', '0101')
            dem_value_ca = get_value_by_filial(df, 'Demanda no período', '0104')
            dem_value_po = get_value_by_filial(df, 'Demanda no período', '0103')
            dem_value_pa = get_value_by_filial(df, 'Demanda no período', '0105')

            ind_value_m = get_value_by_filial(df, 'Ind. Stk', '0101', False)
            ind_value_ca = get_value_by_filial(df, 'Ind. Stk', '0104', False)
            ind_value_po = get_value_by_filial(df, 'Ind. Stk', '0103', False)
            ind_value_pa = get_value_by_filial(df, 'Ind. Stk', '0105', False)

            # Set all labels
            self.ui.ind_stk_m.setText(f"Indicador de Estoque: {ind_value_m}")
            self.ui.ind_stk_c.setText(f"Indicador de Estoque: {ind_value_ca}")
            self.ui.ind_stk_p.setText(f"Indicador de Estoque: {ind_value_po}")
            self.ui.ind_stk_pa.setText(f"Indicador de Estoque: {ind_value_pa}")
            self.ui.agrup_label.setText(f"Agrupamento: {group_value}")
            self.ui.group_label.setText(f"Grupo: {item_group_value} - {group_desc}")
            self.ui.desc_label.setText(f"Descrição: {desc_value}")
            self.ui.min.setText(f"Min: {min_value_m}")
            self.ui.min_c.setText(f"Min: {min_value_ca}")
            self.ui.min_p.setText(f"Min: {min_value_po}")
            self.ui.min_pa.setText(f"Min: {min_value_pa}")
            self.ui.max.setText(f"Máx: {max_value_m}")
            self.ui.max_c.setText(f"Máx: {max_value_ca}")
            self.ui.max_p.setText(f"Máx: {max_value_po}")
            self.ui.max_pa.setText(f"Máx: {max_value_pa}")
            self.ui.seguranca.setText(f"Segurança: {seg_value_m}")
            self.ui.seguranca_c.setText(f"Segurança: {seg_value_ca}")
            self.ui.seguranca_p.setText(f"Segurança: {seg_value_po}")
            self.ui.seguranca_pa.setText(f"Segurança: {seg_value_pa}")
            self.ui.nota.setText(f"Nota: {nota_value_m}")
            self.ui.nota_c.setText(f"Nota: {nota_value_ca}")
            self.ui.nota_p.setText(f"Nota: {nota_value_po}")
            self.ui.nota_pa.setText(f"Nota: {nota_value_pa}")
            self.ui.call.setText(f"Call: {call_value_m}")
            self.ui.call_c.setText(f"Call: {call_value_ca}")
            self.ui.call_p.setText(f"Call: {call_value_po}")
            self.ui.call_pa.setText(f"Call: {call_value_pa}")
            self.ui.dem.setText(f"Dem: {dem_value_m}")
            self.ui.dem_c.setText(f"Dem: {dem_value_ca}")
            self.ui.dem_p.setText(f"Dem: {dem_value_po}")
            self.ui.dem_pa.setText(f"Dem: {dem_value_pa}")

        except Exception as e:
            self.clear_labels()
            logger.info(f"using code instead of group code. {e} ")
            item_group_value = df.iloc[0]['Grupo']
            group_desc = df.iloc[0]['DescricaoGrupo']
            desc_value = df.iloc[0]['Descrição']
            self.ui.agrup_label.setText(f"Agrupamento: ")
            self.ui.group_label.setText(f"Grupo: {item_group_value} - {group_desc}")
            self.ui.desc_label.setText(f"Descrição: {desc_value}")
            self.ui.min.setText(f"Min:")
            self.ui.max.setText(f"Máx:")
            self.ui.seguranca.setText(f"Segurança:")
            self.ui.seguranca_c.setText(f"Segurança:")
            self.ui.seguranca_p.setText(f"Segurança:")
            self.ui.seguranca_pa.setText(f"Segurança:")
            self.ui.nota.setText(f"Nota:")

        self.display_dataframe(df)

    def display_dataframe(self, df):
        """
        Display the dataframe in the QTableWidget.
        """
        if df is None or df.empty:
            self.ui.search_result.clearContents()
            self.ui.search_result.setRowCount(1)
            return

            # Rename columns
        columns_to_keep = ["Código", "Quantidade", "Filial"]
        df_filter = df[columns_to_keep].copy()

        # Aggregate the quantities for each 'Código' within each 'Filial'
        grouped_df = df_filter.groupby(['Código', 'Filial'], as_index=False)['Quantidade'].sum()

        # Pivot the table to get the quantity for each 'Código' per 'Filial'
        pivot_df = grouped_df.pivot(index='Código', columns='Filial', values='Quantidade').fillna(0)

        # Set the row count
        self.ui.search_result.setRowCount(pivot_df.shape[0])

        # Define the column index for branches
        matriz_col_index = 1
        cariacica_col_index = 2
        pocone_col_index = 3
        paraua_col_index = 4

        # Populate the QTableWidget
        for row_index, (codigo, row_data) in enumerate(pivot_df.iterrows()):
            # Set code value
            self.ui.search_result.setItem(row_index, 0, QTableWidgetItem(str(codigo)))

            # Set quantities in respective columns
            self.ui.search_result.setItem(row_index, matriz_col_index, QTableWidgetItem(str(row_data.get('0101', 0))))
            self.ui.search_result.setItem(row_index, cariacica_col_index,
                                          QTableWidgetItem(str(row_data.get('0104', 0))))
            self.ui.search_result.setItem(row_index, pocone_col_index, QTableWidgetItem(str(row_data.get('0103', 0))))
            self.ui.search_result.setItem(row_index, paraua_col_index, QTableWidgetItem(str(row_data.get('0105', 0))))

    def clear_labels(self):
        self.ui.ind_stk_m.setText(f"Indicador de Estoque: ")
        self.ui.ind_stk_c.setText(f"Indicador de Estoque: ")
        self.ui.ind_stk_p.setText(f"Indicador de Estoque: ")
        self.ui.ind_stk_pa.setText(f"Indicador de Estoque: ")
        self.ui.agrup_label.setText(f"Agrupamento: ")
        self.ui.group_label.setText(f"Grupo: ")
        self.ui.desc_label.setText(f"Descrição: ")
        self.ui.min.setText(f"Min: ")
        self.ui.min_c.setText(f"Min: ")
        self.ui.min_p.setText(f"Min: ")
        self.ui.min_pa.setText(f"Min: ")
        self.ui.max.setText(f"Máx: ")
        self.ui.max_c.setText(f"Máx: ")
        self.ui.max_p.setText(f"Máx: ")
        self.ui.max_pa.setText(f"Máx: ")
        self.ui.seguranca.setText(f"Segurança: ")
        self.ui.seguranca_c.setText(f"Segurança: ")
        self.ui.seguranca_p.setText(f"Segurança: ")
        self.ui.seguranca_pa.setText(f"Segurança: ")
        self.ui.nota.setText(f"Nota: ")
        self.ui.nota_c.setText(f"Nota: ")
        self.ui.nota_p.setText(f"Nota: ")
        self.ui.nota_pa.setText(f"Nota: ")
        self.ui.call.setText(f"Call: ")
        self.ui.call_c.setText(f"Call: ")
        self.ui.call_p.setText(f"Call: ")
        self.ui.call_pa.setText(f"Call: ")
        self.ui.dem.setText(f"Dem: ")
        self.ui.dem_c.setText(f"Dem: ")
        self.ui.dem_p.setText(f"Dem: ")
        self.ui.dem_pa.setText(f"Dem: ")


class Analysis_Report_Logic(BaseLogic):
    def __init__(self, ui):
        super().__init__(ui)
        self.setup_connections()

    def setup_connections(self):
        self.ui.start_table_button.clicked.connect(self.start_table)

    def start_table(self):
        filial = self.ui.table_filial_select.currentText()
        periodo = self.ui.table_periodo_select.currentText()

        self.download_thread = DownloadThread(create_report, filial, periodo, False)
        self.download_thread.progress_started.connect(self.start_progress)
        self.download_thread.progress_stopped.connect(self.stop_progress)
        self.download_thread.finished.connect(self.on_thread_finished)
        self.download_thread.start()


class Table_Search_Logic(BaseLogic):

    def __init__(self, ui):
        super().__init__(ui)
        self.checkboxes = None
        self.table_name = None
        self.column_names = None
        self.setup_connections()

    def setup_connections(self):
        self.ui.fetch_tables_download_button_2.clicked.connect(self.get_columns)
        self.ui.fetch_tables_download_button.clicked.connect(self.get_table)
        self.ui.select_all_button.clicked.connect(self.select_all_checkboxes)
        self.ui.clear_selection_button.clicked.connect(self.clear_checkboxes)

    def get_columns(self):
        table_name = self.ui.lineEdit_fetch_tables.text().upper()

        self.download_thread = DownloadThread(get_table_columns, table_name)
        self.download_thread.progress_started.connect(self.start_progress)
        self.download_thread.progress_stopped.connect(self.stop_progress)

        self.download_thread.finished_with_result.connect(self.update_label_checkboxes)

        self.download_thread.start()
        self.table_name = table_name

    def update_label_checkboxes(self, columns):
        self.clear_scroll_area()
        if not columns:
            return
        else:
            # Get the existing layout from the scroll area's widget
            layout = self.ui.checkbox_contents_scroll.layout()
            count_columns = len(columns)

            # If the layout does not exist, create it
            if layout is None:
                layout = QVBoxLayout(self.ui.checkbox_contents_scroll)
                self.ui.checkbox_contents_scroll.setLayout(layout)

            # Create checkboxes based on columns
            self.checkboxes = {}
            for column in columns:
                checkbox = QCheckBox(column)
                self.checkboxes[column] = checkbox
                layout.addWidget(checkbox)

    def select_all_checkboxes(self):
        if self.checkboxes is not None:
            for checkbox in self.checkboxes.values():
                checkbox.setChecked(True)

    def clear_checkboxes(self):
        if self.checkboxes is not None:
            for checkbox in self.checkboxes.values():
                checkbox.setChecked(False)

    def clear_scroll_area(self):
        inner_widget = self.ui.checkbox_contents_scroll
        if inner_widget is not None:
            layout = inner_widget.layout()
            if layout:
                while layout.count():
                    item = layout.takeAt(0)
                    if item.widget():
                        item.widget().deleteLater()

    def update_selected_columns(self):
        selected_columns = []
        if self.checkboxes is not None:
            for column, checkbox in self.checkboxes.items():
                if checkbox.isChecked():
                    selected_columns.append(column)
        self.column_names = selected_columns

    def get_table(self):
        self.update_selected_columns()

        if self.column_names and self.table_name:
            columns = self.column_names
            table = self.table_name
            columns_str = ', '.join(columns)

            self.download_thread = DownloadThread(download_save_table, columns_str, table)
            self.download_thread.progress_started.connect(self.start_progress)
            self.download_thread.progress_stopped.connect(self.stop_progress)
            self.download_thread.finished.connect(self.on_thread_finished)
            self.download_thread.start()

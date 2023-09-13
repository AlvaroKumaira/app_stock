import tkinter as tk
from tkinter import ttk, filedialog
from tkcalendar import DateEntry
import threading
import logging
from database_functions.funcoes_base import export_to_mysql
from main_functions.download_tabelas import download_tabelas
from main_functions.sugestao_compra import create_final_df
from main_functions.busca_produtos import search_function

# Configure logger for the module
logger = logging.getLogger(__name__)

class HubWindow(tk.Tk):
    """
    Main hub of the application, providing access to various functionalities.
    This class inherits from the tkinter's main window (Tk).
    """
    
    def __init__(self):
        """
        Initialize the HubWindow class.
        Set the window properties and call the method to initialize UI components.
        """
        super().__init__()
        self.geometry("500x350")
        self.resizable(True, True)
        self.title("Gestão de Inventário")
        self.current_frame = None
        self.show_hub()

    def clear_frame(self):

        if self.current_frame:
            self.current_frame.destroy()
        self.current_frame = tk.Frame(self)
        self.current_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    def show_hub(self):
        """
        Initializes the UI components for the main hub window.
        Components:
        - Buttons to open various other windows and functionalities.
        """
        self.clear_frame()
        
        # Create buttons with corresponding actions and place them in the window
        tk.Button(self.current_frame, text="Download Tabelas", command=self.show_download_tables_ui, relief="groove").grid(row=0, column=0, padx=10, pady=10, sticky='w')
        tk.Button(self.current_frame, text="Buscar Agrupamento", command=self.show_search_ui, relief="groove").grid(row=1, column=0, padx=10, pady=10, sticky='w')
        tk.Button(self.current_frame, text="Sugestão de Compra", command=self.show_sugestao_ui, relief="groove").grid(row=2, column=0, padx=10, pady=10, sticky='w')
        tk.Button(self.current_frame, text="Sugestão de Importação", command=self.open_import_window, relief="groove").grid(row=3, column=0, padx=10, pady=10, sticky='w')
        tk.Button(self.current_frame, text="Atualizar parâmetros", command=self.update_params, relief="groove").grid(row=4, column=0, padx=10, pady=10, sticky='w')

    def update_params(self):
        """
        Prompt the user to select an excel file and then export its content 
        to the MySQL 'params' table.
        """
        
        file_path = filedialog.askopenfilename(title="Abrir excel de parâmetros")

        if file_path:  # Check if a file was selected
            tablename = "params"
            try:
                export_to_mysql(file_path, tablename)
                logger.info(f"Exported data from {file_path} to MySQL table {tablename}")
            except Exception as e:
                logger.error(f"Error exporting data from {file_path} to MySQL: {e}")

    def show_download_tables_ui(self):
        """Display the download tables UI in the main frame"""
        self.clear_frame()

        # Initialize the DownloadTablesUI view in the current_frame
        dtui = DownloadTablesUI(self.current_frame, self.show_hub)
        dtui.pack(fill=tk.BOTH, expand=True)  # need to pack the frame to ensure it displays

    def show_sugestao_ui(self):
        """Display the Sugestao UI in the main frame"""
        self.clear_frame()

        # Initialize the DownloadTablesUI view in the current_frame
        sutui = SugestaoUI(self.current_frame, self.show_hub)
        sutui.pack(fill=tk.BOTH, expand=True)

    def show_search_ui(self):
        """Display the Search UI in the main frame"""
        self.clear_frame()

        # Initialize the DownloadTablesUI view in the current_frame
        seui = SearchUI(self.current_frame, self.show_hub)
        seui.pack(fill=tk.BOTH, expand=True)

    def open_import_window(self):
        """
        Display the UI dedicated to import suggestions.
        Note: Implementation of this function is currently pending.
        """
        pass

class DownloadTablesUI(tk.Frame):
    """
    A UI class for downloading tables based on user selections.
    This class inherits from the tkinter Frame widget.
    """

    def __init__(self, master, return_callback):
        """
        Initialize the DownloadTablesUI class.
        Set the window properties and call the method to initialize UI components.
        """
        super().__init__(master)
        self.return_callback = return_callback
        self.init_ui()

    def init_ui(self):
        """
        Initializes the UI components for the Download Tables window.
        Components:
        - Dropdown for selecting filial
        - Date entry for selecting the start date for pedidos table
        - Checkbuttons for selecting which tables to download
        - Button to initiate the table download
        - Progress bar to show the operation's progress
        """

        # Dropdown for filial selection
        self.selected_filial = tk.StringVar(self, value="0101")
        filial_options = ["0101", "0103", "0104", "0105"]
        filial_dropdown = tk.OptionMenu(self, self.selected_filial, *filial_options)

        # Date selection widget for the pedidos query
        date_label = tk.Label(self, text="Inicio - Pedidos:")
        self.pedidos_date_entry = DateEntry(self, date_pattern='dd/mm/yyyy', locale='pt_BR.utf8', showcalendar=True)
        # Date selection widget for the faturamento query
        faturamento_date_label = tk.Label(self, text="Inicio - Faturamento:")
        self.faturamento_date_entry = DateEntry(self, date_pattern='dd/mm/yyyy', locale='pt_BR.utf8', showcalendar=True)

        # Button to initiate the table download
        download_button = tk.Button(self, text="Download", command=self.start_download_data)
        back_button = tk.Button(self, text="Voltar", command=self.return_callback)

        # Checkbuttons to allow user to select which tables to download
        self.saldo_var = tk.BooleanVar(value=True)
        self.pedidos_var = tk.BooleanVar(value=True)
        self.faturamento_var = tk.BooleanVar(value=True)
        saldo_check = tk.Checkbutton(self, text="Saldo Analítico", variable=self.saldo_var)
        pedidos_check = tk.Checkbutton(self, text="Pedidos", variable=self.pedidos_var)
        faturamento_check = tk.Checkbutton(self, text="Faturamento", variable=self.faturamento_var)

        # Progress bar to provide visual feedback during data download
        self.progress_bar = ttk.Progressbar(self, mode='indeterminate', length=200)

        # Arrange the UI components using grid layout
        filial_dropdown.grid(row=0, column=0)
        date_label.grid(row=1, column=0)
        self.pedidos_date_entry.grid(row=2, column=0)
        saldo_check.grid(row=0, column=1, sticky='w')
        pedidos_check.grid(row=1, column=1, sticky='w')
        faturamento_check.grid(row=2, column=1, sticky='w')
        download_button.grid(row=0, column=2)
        back_button.grid(row=6, column=6)
        self.progress_bar.grid(row=5, column=0, columnspan=3, padx=10, pady=10)
        faturamento_date_label.grid(row=3, column=0)
        self.faturamento_date_entry.grid(row=4, column=0)

    def start_download_data(self):
        """
        Initiate the downloading of the selected tables.
        Downloading is performed in a separate thread to prevent blocking the main UI thread.
        """

        # Fetch user selections for tables, filial, and date
        saldo = self.saldo_var.get()
        pedidos = self.pedidos_var.get()
        faturamento = self.faturamento_var.get()
        filial = self.selected_filial.get()
        pedidos_selected_date = self.pedidos_date_entry.get_date()
        faturamento_selected_date = self.faturamento_date_entry.get_date()


        # Start the progress bar animation
        self.progress_bar.start()

        # Inner function to download data and stop the progress bar once completed
        def download_and_update_progress():
            try:
                download_tabelas(filial, saldo, pedidos, faturamento, pedidos_selected_date, faturamento_selected_date)
                logger.info(f"Downloaded data for filial {filial} up to date.")
            except Exception as e:
                logger.error(f"Error during data download for filial {filial}: {e}")
            finally:
                self.progress_bar.stop()  # Stop the progress bar

        # Start the downloading in a separate thread to prevent freezing the main UI
        download_thread = threading.Thread(target=download_and_update_progress)
        download_thread.start()

class SugestaoUI(tk.Frame):
    """
    A UI class for handling Sugestão de Compra operations.
    This class inherits from the tkinter Toplevel widget.
    """
    
    def __init__(self, master, return_callback):
        """
        Initialize the SugestaoUI class.
        Set the window properties and call the method to initialize UI components.
        """
        super().__init__(master)
        self.return_callback = return_callback
        self.init_ui()

    def init_ui(self):
        """
        Initializes the UI components for the Sugestão de Compra window.
        Components:
        - Dropdown for selecting filial
        - Button to initiate the download
        - Progress bar to show the operation's progress
        - A Back button to return to the main hub
        """
        
        # Dropdown for selecting filial
        self.selected_filial = tk.StringVar(self, value="0101")
        filial_options = ["0101", "0103", "0104", "0105"]
        filial_dropdown = tk.OptionMenu(self, self.selected_filial, *filial_options)
        
        # Button to initiate the download
        download_button = tk.Button(self, text="Download", command=self.download_thread)
        back_button = tk.Button(self, text="Voltar", command=self.return_callback)

        # Progress bar to show the user that the script is running
        self.progress_bar = ttk.Progressbar(self, mode='indeterminate', length=200)
        
        # Layout all widgets using a grid
        filial_dropdown.grid(row=0, column=0)
        download_button.grid(row=0, column=2, rowspan=1)
        back_button.grid(row=0, column=3)
        self.progress_bar.grid(row=3, column=0, columnspan=3, padx=10, pady=10)

    def download_suges(self):
        """
        Initiates the download of the tables.
        Calls the create_final_df function, which is a part of sugestao_compra script.
        """

        logger.info("Starting data download for filial: %s", self.selected_filial.get())
        
        # Start the progress bar
        self.progress_bar.start()
        
        # Get the selected filial
        filial = self.selected_filial.get()

        try:
            create_final_df(filial)
            logger.info("Data download completed for filial: %s", filial)
            
        except Exception as e:
            logger.error("An error occurred during data download: %s", str(e))

        finally:
            # Stop the progress bar after the download has completed
            self.progress_bar.stop()
    
    def download_thread(self):
        """
        Create a thread for downloading data to prevent blocking the main UI thread.
        """
        
        logger.info("Starting download thread...")
        download_thread = threading.Thread(target=self.download_suges)
        download_thread.start()  # Start the thread

class SearchUI(tk.Frame):
    """
    A UI class for item searching functionality.
    This class inherits from the tkinter Toplevel widget.
    """
    
    def __init__(self, master, return_callback):
        """
        Initialize the SearchUI class.
        Set the window properties and call the method to initialize UI components.
        """
        super().__init__(master)
        self.return_callback = return_callback
        self.init_ui()

    def init_ui(self):
        """
        Initializes the UI components for the item search window.
        Components:
        - Entry field for product ID
        - Button to initiate the search
        - Labels and Treeview to display the results
        """
        
        # Entry field for product ID
        self.product_id_var = tk.StringVar()
        product_id_label = tk.Label(self, text="Código:")
        self.product_id_entry = tk.Entry(self, textvariable=self.product_id_var)
        
        # Button to initiate the search
        start_button = tk.Button(self, text="Buscar", command=self.start_search)
        back_button = tk.Button(self, text="Voltar", command=self.return_callback)

        # Display area for the results
        self.results_frame = tk.Frame(self)
        self.group_label = tk.Label(self.results_frame)
        self.desc_label = tk.Label(self.results_frame)
        
        # Treeview to display the search results
        self.tree = ttk.Treeview(self.results_frame, columns=("Código", "Quantidade"))
        self.setup_treeview()

        # Layout all widgets using a grid
        product_id_label.grid(row=0, column=0, padx=10, pady=10)
        back_button.grid(row=0, column=9, padx=10, pady=10)
        self.product_id_entry.grid(row=1, column=0, padx=10, pady=10)
        start_button.grid(row=2, column=0, padx=10, pady=10)
        self.results_frame.grid(row=3, column=0, columnspan=4, padx=10, pady=10)
        self.group_label.grid(row=0, column=0, padx=10, pady=10)
        self.desc_label.grid(row=0, column=1, padx=10, pady=10)
        self.tree.grid(row=1, column=0, columnspan=4, padx=10, pady=10)

    def setup_treeview(self):
        """Configure the properties of the Treeview."""
        self.tree["show"] = "headings"
        self.tree.heading("Código", text="Código")
        self.tree.heading("Quantidade", text="Quantidade")
        self.tree.column("Código", width=200)
        self.tree.column("Quantidade", width=200)

    def start_search(self):
        """
        Handle the search logic.
        Calls the search_function with the product ID and updates the UI with the results.
        """
        product_id = self.product_id_var.get()
        df = search_function(product_id)

        if df is not None and not df.empty:
            self.update_labels(df)
            self.display_dataframe(df)

    def update_labels(self, df):
        """
        Update the group and description labels based on dataframe values.
        """
        group_value = df.iloc[0]['B1_ZGRUPO']
        desc_value = df.iloc[0]['B1_DESC']
        self.group_label.config(text=f"Agrupamento: {group_value}")
        self.desc_label.config(text=f"Descrição: {desc_value}")
        df.drop(columns=['B1_ZGRUPO', 'B1_DESC'], inplace=True)

    def display_dataframe(self, df):
        """
        Display the dataframe in the Treeview.
        """
        column_names = {"B1_COD": "Código", "B2_QATU": "Quantidade"}
        df.rename(columns=column_names, inplace=True)
        
        # Clear previous data in Treeview
        for row in self.tree.get_children():
            self.tree.delete(row)

        # Insert new data into the Treeview
        for index, row in df.iterrows():
            self.tree.insert("", "end", values=list(row))



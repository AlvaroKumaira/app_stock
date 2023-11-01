import logging
import os
from user_interface.main_ui import MainWindowLogic
from PyQt5.QtWidgets import QApplication
from database_functions.params_update import save_excel_locally

# Set up logging configurations.
logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


def main():
    """
    Entry point for the application.

    This function initializes the MainWindow and starts the PyQt event loop.
    Any unexpected errors during this process are logged and then raised.
    """
    try:
        # Create a PyQt application instance.
        app = QApplication([])

        # Update the Excel file from the shared folder before starting the application
        shared_folder_path = "Z:\\09 - Pecas\\Sgc"
        file_name = "Dados_Sug.xlsx"
        save_excel_locally(shared_folder_path, file_name)

        # Create a MainWindowExtended_Download_Tables instance and show it.
        window = MainWindowLogic()
        window.show()

        # Start the PyQt event loop.
        app.exec_()
        logging.info(f"Current working directory: {os.getcwd()}")

    except Exception as e:
        logging.error(f"An unexpected error occurred while creating the session: {e}")
        raise


if __name__ == "__main__":
    # If the script is executed as the main module, call the main function.
    main()

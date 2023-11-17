import logging
import os
from user_interface.main_ui import MainWindowLogic
from PyQt5.QtWidgets import QApplication

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

        # Create a MainWindow
        window = MainWindowLogic()
        window.show()

        # Start the PyQt event loop.
        app.exec_()

    except Exception as e:
        logging.error(f"An unexpected error occurred while creating the session: {e}")
        raise


if __name__ == "__main__":
    # If the script is executed as the main module, call the main function.
    main()

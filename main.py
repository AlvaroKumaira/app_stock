import logging
from user_interface.ui_classes import HubWindow

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
    
    This function initializes the HubWindow and starts the Tkinter event loop.
    Any unexpected errors during this process are logged and then raised.
    """
    try:
        # Create a HUB window instance.
        hub_window = HubWindow()
        
        # Start the Tkinter event loop.
        hub_window.mainloop()
    except Exception as e:
        logging.error(f"An unexpected error occurred while creating the session: {e}")
        raise


if __name__ == "__main__":
    # If the script is executed as the main module, call the main function.
    main()
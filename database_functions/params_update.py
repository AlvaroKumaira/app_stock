import os
import pandas as pd
import subprocess
import logging
import shutil

logger = logging.getLogger(__name__)


def is_network_reachable(network_name="CENTRO OESTE"):
    try:
        # Execute a command to get the names of connected network profiles
        networks = subprocess.check_output("netsh wlan show interfaces", shell=True).decode('utf-8', errors='ignore')
        # Check if the specified network name exists in the command output
        return network_name in networks
    except Exception as e:
        logger.error(f"An error occurred while checking the network connection: {e}")
        return False


def save_excel_locally(file_name, data=None, local_folder="params", shared_folder_path=None):
    # Ensure the local folder exists
    if not os.path.exists(local_folder):
        os.makedirs(local_folder)

    local_file_path = os.path.join(local_folder, file_name)

    # If 'data' is a DataFrame, save it directly to Excel
    if isinstance(data, pd.DataFrame):
        try:
            data.to_excel(local_file_path, index=False)
            logger.info(f"DataFrame saved to {local_file_path}")
        except Exception as e:
            logger.error(f"An error occurred while saving the DataFrame: {e}")
        return

    # If a shared_folder_path is provided and 'data' is None, attempt to copy file from network
    if shared_folder_path and data is None and is_network_reachable():
        try:
            # Try to connect to the shared folder and get the file
            network_file_path = os.path.join(shared_folder_path, file_name)
            shutil.copy(network_file_path, local_file_path)
            logger.info(f"File copied to {local_file_path}")
        except FileNotFoundError:
            logger.error(f"No such file or directory: {network_file_path}")
        except Exception as e:
            logger.error(f"An error occurred while copying the file: {e}")
    elif not shared_folder_path and data is None:
        logger.error("No shared folder path provided and no DataFrame to save.")
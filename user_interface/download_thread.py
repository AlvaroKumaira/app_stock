from PyQt5.QtCore import QThread, pyqtSignal
import logging

# Set up logging
logger = logging.getLogger(__name__)


class DownloadThread(QThread):
    progress_started = pyqtSignal()
    progress_stopped = pyqtSignal()
    finished_with_result = pyqtSignal(object)

    def __init__(self, func, *args, **kwargs):
        super(DownloadThread, self).__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        try:
            self.progress_started.emit()
            result = self.func(*self.args, **self.kwargs)
            self.finished_with_result.emit(result)
            logger.info(f"Successfully executed {self.func.__name__}")
        except Exception as e:
            logger.error(f"Error during execution of {self.func.__name__}: {e}")
        finally:
            self.progress_stopped.emit()

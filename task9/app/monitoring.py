import os
import time


from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from scripts.logger import logger


DIRECTORY_TO_WATCH = f"./data"
SCRIPT_TO_EXECUTE = f"./scripts/log_processor.py"


def run_script_on_new_csv(directory: str, script_path: str) -> None:
    """
    Monitoring to find new data.csv.
    If file download to 'data' folder then started processing data.
    :param directory: str
    :param script_path: str
    :return: None
    """
    class NewCSVHandler(FileSystemEventHandler):
        def on_created(self, event):
            if event.is_directory:
                return

            filename, extension = os.path.splitext(os.path.basename(event.src_path))
            if extension.lower() == '.csv':
                logger.info(f"New CSV file detected: {event.src_path}")
                os.system(f"python {script_path} '{filename}{extension.lower()}'")

    event_handler = NewCSVHandler()
    observer = Observer()
    observer.schedule(event_handler, path=directory, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


run_script_on_new_csv(DIRECTORY_TO_WATCH, SCRIPT_TO_EXECUTE)

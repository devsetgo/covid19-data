from fetch_data_files import start_downloads
from com_lib.log_config import config_log
from loguru import logger
from process_world_data import main as process_main

config_log()

def main():
    logger.info("start")
    start_downloads()
    logger.info("start world processing")
    process_main()

if __name__ == "__main__":
    main()

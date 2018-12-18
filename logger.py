import logging, os
from constants import LOG_FILE_NAME

logging.basicConfig(
	format='%(asctime)s [%(levelname)-5.5s] %(message)s',
	datefmt='%d-%b-%y %H:%M:%S'
	)
file_name = os.path.join(os.path.dirname(__name__), LOG_FILE_NAME)
print(file_name)
logging.FileHandler(file_name)

logger = logging.getLogger('__name__')

logger.info("[+] logger initiated")
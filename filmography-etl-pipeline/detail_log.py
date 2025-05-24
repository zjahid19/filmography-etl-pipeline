import logging

# creating logging object
logger = logging.getLogger(__name__)

# seting log level
logger.setLevel(logging.INFO)

# creating string format
format_string = "%(asctime)s-%(levelname)s-%(message)s"

formatter = logging.Formatter(format_string)

# creating handler
handler = logging.StreamHandler()

# adding string format to hadler
handler.setFormatter(formatter)

# adding handller to logger
logger.addHandler(handler)

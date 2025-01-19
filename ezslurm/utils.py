import logging


def setup_logger(
    filename=None,
    file_level=logging.INFO,
    stream_level=logging.INFO,
    fmt="[%(levelname).1s] %(asctime)s | %(filename)s:%(lineno)d | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
):
    formatter = logging.Formatter(fmt, datefmt=datefmt)
    file_handler = (
        logging.FileHandler(filename, mode="w")
        if filename is not None
        else logging.NullHandler()
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(file_level)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(stream_level)

    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt=datefmt,
        handlers=[file_handler, stream_handler],
    )

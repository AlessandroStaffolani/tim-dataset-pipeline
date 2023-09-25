import json
import math
import os


def format_bytes(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return '{:.2f} {}'.format(s, size_name[i])


ROOT_DIR = os.path.abspath(os.path.relpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')))


def create_directory(path):
    """
    Create the directory if not exist
    Parameters
    ----------
    path: str
        directory path
    Returns
    -------
        return the string path generated
    """
    if path != '':
        os.makedirs(path, exist_ok=True)
    return path


def create_directory_from_filepath(filepath):
    """
    Create the directory if not exist from a filepath (that is the path plus the filename)
    Parameters
    ----------
    filepath: str
        file path
    Returns
    -------
    str
        return the path (up to final folder) created
    """
    components = filepath.split('/')
    path = '/'.join(components[0: -1])
    create_directory(path)
    return path


def print_status(current, total, pre_message='', loading_len=20, unit='', current_formatted=None, total_formatted=None):
    current_str = current_formatted if current_formatted is not None else f'{current}{unit}'
    total_str = total_formatted if total_formatted is not None else f'{total}{unit}'

    perc = int((current * loading_len) / total)
    message = f'{pre_message}:\t{"#" * perc}{"." * (loading_len - perc)}\t{current_str}/{total_str}'
    print(message, end='\r')


def load_json_file(path):
    with open(path, 'r') as f:
        return json.load(f)

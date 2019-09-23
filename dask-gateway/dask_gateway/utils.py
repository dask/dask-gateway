import os


def format_template(x):
    if isinstance(x, str):
        return x.format(**os.environ)
    return x

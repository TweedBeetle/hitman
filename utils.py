def is_ascii_char(c):
    return ord(c) < 128


def filter_non_ascii(s):
    return ''.join(filter(is_ascii_char, list(s)))
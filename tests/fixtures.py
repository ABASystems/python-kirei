import tempfile


FIELDS = {
    'A': {'type': str},
    'B': {'type': str},
    'C': {'type': str},
    'D': {'type': str},
}


def gen_empty_csv():
    file = tempfile.NamedTemporaryFile()
    file.write(b'A,B,C,D\n')
    file.flush()
    return file


def gen_linear_csv(base=None):
    if base is None:
        base = 0
    file = gen_empty_csv()
    for ii in range(10):
        file.write(bytes(','.join(map(str, [ii, base + 100 + ii, base + 200 + ii, base + 300 + ii])) + '\n', 'utf8'))
    file.flush()
    return file

import datetime
import decimal
import json
import unicodedata
import uuid

from fastavro._write_py import WRITERS
from fastavro.write import Writer


def _avro_format(inp):
    if isinstance(inp, uuid.UUID):
        return str(inp)
    if isinstance(inp, (dict, list, tuple)):
        return json.dumps(inp)
    if type(inp) == datetime.datetime:
        # datetime is a subclass of date but the api for isoformat is different
        # so we can't use isinstance here
        return inp.isoformat(" ")
    if isinstance(inp, datetime.date):
        return inp.isoformat()
    if isinstance(inp, datetime.timedelta):
        return (datetime.datetime.min + inp).time().isoformat()
    if isinstance(inp, decimal.Decimal):
        return str(inp)
    if isinstance(inp, str):
        return (
            unicodedata.normalize("NFKD", inp).encode("ascii", "ignore").decode("ascii")
        )
    return inp


def _format_row(inp):
    return {k: _avro_format(v) for k, v in inp.items()}


def write_avro_file(f, results_iter, fields, table, max_size=100 * 1024 ** 2):
    """Takes a database result set (list of dicts) and writes an avro file
    up to a particular size. If the schema 'name' is the same as an Avro data
    type (WRITERS.keys()) everything will break for no apparent reason. 'name'
    isn't even really used.

    Returns complete, row_count

    complete is true if the entire results_iter has been drained -- false if
    there are more records to be processed.

    row_count is the number of items written

    max_size is limit at which we should start writing another file.
    """

    if table in WRITERS:
        table += "zzz"

    schema = {"type": "record", "name": table, "fields": fields}

    writer = Writer(f, schema)

    row_count = 0
    complete = False

    try:
        # writer.io buffers before writing
        while f.tell() + writer.io.tell() < max_size:
            writer.write(_format_row(next(results_iter)))
            row_count += 1
    except StopIteration:
        complete = True
    finally:
        writer.flush()

    return complete, row_count

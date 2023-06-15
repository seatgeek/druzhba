import datetime
import decimal
import json
import unicodedata
import uuid

from fastavro._write_py import WRITERS
from fastavro.write import Writer

MAX_VARCHAR_SIZE = 65535

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
    if isinstance(inp, datetime.time):
        return inp.isoformat()
    if isinstance(inp, decimal.Decimal):
        return str(inp)
    if isinstance(inp, str):
        return (
            unicodedata.normalize("NFKD", inp).encode("ascii", "ignore").decode("ascii")
        )
    return inp


def _redshift_format(inp):
    if isinstance(inp, str):
        # The copy command already has TRUNCATECOLUMNS turned on which will do this for us 
        # however this protects us from the case where a columns value is ridiciously large 
        # preventing the row from being ingested.
        return inp[:MAX_VARCHAR_SIZE]
    return inp


def _format_value(inp):
    formatted_for_avro = _avro_format(inp)
    return _redshift_format(formatted_for_avro)


def _format_row(inp):
    return {k: _format_value(v) for k, v in inp.items()}


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
        while f.tell() + writer.io.tell() < max_size:  # pylint: disable=no-member
            writer.write(_format_row(next(results_iter)))
            row_count += 1
    except StopIteration:
        complete = True
    finally:
        writer.flush()

    return complete, row_count

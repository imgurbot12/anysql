"""
AnySQL Escape/Unescape Implementation
"""
import math
import datetime
from typing import *

from .interface import *

#** Variables **#
__all__ = ['mogrify', 'prepare', 'escape_arg']

#: typehint for valid encoders dictionary
Encoders    = Dict[Type, Callable[[Any, 'Encoders'], str]]
OptEncoders = Optional[Encoders]

#: global cache for already prepared statements
PREPARED_CACHE: Dict[str, Prepared] = {}

#** Functions **#

def mogrify(query: Stmt, args: Args = None, cache: bool = True) -> Query:
    """
    escape query arguments and generate finalized query-string

    :param query: sql query-string
    :param args:  arguments to pass to the query-string
    :param cache: allow cache of prepared statement
    :return:      formatted and complete sql-query statement
    """
    # skip mogrify for completed query
    if isinstance(query, Query):
        if args:
            raise ProgrammingError(f'cannot use args w/ pregenerated Query')
        return query
    # translate and generate prepared and unprepared queries
    query = prepare(query, cache)
    if args is not None:
        if query.mode is ArgMode.NONE:
            if args:
                raise ProgrammingError(f'query cannot accept any arguments')
        elif query.mode is ArgMode.ARGS:
            if not isinstance(args, Sequence):
                raise ProgrammingError(f'query args must be a valid sequence')
            args  = tuple((escape_arg(arg) for arg in args))
            query = query.format(*args)
        elif query.mode is ArgMode.KWARGS:
            if not isinstance(args, Mapping):
                raise ProgrammingError('query args must be a valid dict/map')
            args  = {k:escape_arg(v) for k,v in args.items()}
            query = query.format(**args)
    return Query(query)

def is_placeholder(chars: List[str]) -> Optional[str]:
    """check if placeholder and return name if true"""
    if not chars:
        return
    if chars[0] == ':':
        return ''.join(chars[1:])
    if chars[0] == '(' and chars[-2:] == ')s':
        return ''.join(chars[1:-2])
    if chars[0] == '{' and chars[-1] == '}':
        return ''.join(chars[1:-1])
    if len(chars) == 1 and chars[0] == '?':
        return ''
    if len(chars) == 2 and chars == ['%', 's']:
        return ''

def prepare(query: Union[str, Prepared], cache: bool = False) -> Prepared:
    """
    translate placeholders into a single standard format

    :param query: sql query to convert placeholders
    :param cache: cache prepared statements if true
    :return:      translated sql statement w/ valid placeholders
    """
    global PREPARED_CACHE
    # skip translation if already prepared
    if isinstance(query, Prepared):
        return query
    # check cache if enabled
    query = query.strip()
    if cache and query in PREPARED_CACHE:
        return PREPARED_CACHE[query]
    # parse sql query and find placeholders
    original = '' if not cache else str(query)
    placeholders, counter = [], 0
    word, escapes, wordend, quotes = [], 0, False, False
    for c in (query + ' '):
        # track escape characters
        if c == '\\':
            escapes += 1
            continue 
        elif escapes > 0:
            escapes = 0
        # track words in order to detect arg-placeholders
        if c.isspace():
            wordend = len(word) > 0
        elif c in ("'", '"', ''):
            quotes = not quotes if escapes % 2 == 0 else quotes
        elif not quotes and c in ';,.=':
            wordend = len(word) > 0
        elif c not in '()[]{}':
            word.append(c)
        if not wordend:
            continue
        # translate word if placeholder and rest word tracker
        name = is_placeholder(word)
        if name is not None:
            if not name:
                name     = str(counter)
                counter += 1
            elif counter > 0:
                raise ProgrammingError(f'cannot mix unnamed and named vars')
            placeholders.append((f'{{{name}}}', ''.join(word)))
        word.clear()
        wordend = False
    # replace various placeholders w/ standard formatting placeholder
    for pholder, match in placeholders:
        query = query.replace(match, pholder, 1)
    # determine argument mode
    query = Prepared(query)
    if not placeholders:
        query.mode = ArgMode.NONE
    elif counter > 0:
        query.mode = ArgMode.ARGS
    else:
        query.mode = ArgMode.KWARGS
    # cache if cache is enabled
    if cache:
        PREPARED_CACHE[original] = query
    return query

def escape_arg(arg: Any, mapping: OptEncoders = None) -> str:
    """
    escape the given sql argument using the specified mapping

    :param arg:     arbitrary argument value to escape
    :param mapping: mapping used to retrieve escape-function
    :return:        escaped argument string
    """
    mapping = mapping or ENCODER_MAP
    encoder = mapping.get(type(arg))
    if not encoder:
        raise TypeError(f'no escape handler for {type(arg)!r}')
    return encoder(arg, mapping)

def escape_dict(arg: dict, mapping: OptEncoders = None):
    return str({k:escape_arg(v, mapping) for k,v in arg.items()})

def escape_sequence(arg: Sequence, mapping: OptEncoders = None):
    return '(' + ','.join(escape_arg(i, mapping) for i in arg) + ')'

def escape_int(arg: int, _ = None):
    return str(arg)

def escape_bool(arg: bool, _ = None):
    return str(int(arg))

def escape_none(*_):
    return 'NULL'

def escape_float(arg: float, _ = None):
    if math.isnan(arg) or not math.isfinite(arg):
        raise ProgrammingError(f'{arg!r} can not be used in AnySQL')
    f  = repr(arg)
    f += '' if 'e' in f else 'e0'
    return f

def escape_str(arg: str, _: OptEncoders = None):
    return repr(arg)

def escape_bytes(arg: bytes, _ = None):
    return escape_str(arg.decode('ascii', 'surrogateescape'))

def escape_date(arg: datetime.date, _ = None):
    return repr(arg.isoformat())

def escape_datetime(arg: datetime.datetime, _ = None):
    return repr(arg.isoformat(' '))

#** Init **#

#: mapping for escaping python types for query string
ENCODER_MAP: Encoders = {
    int:               escape_int,
    bool:              escape_bool,
    float:             escape_float,
    str:               escape_str,
    bytes:             escape_bytes,
    tuple:             escape_sequence,
    list:              escape_sequence,
    set:               escape_sequence,
    frozenset:         escape_sequence,
    dict:              escape_dict,
    type(None):        escape_none,
    datetime.date:     escape_date,
    datetime.datetime: escape_datetime,
}

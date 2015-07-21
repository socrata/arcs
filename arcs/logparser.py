import re
import urllib
import pandas as pd
from frozendict import frozendict
from functools import partial
from dateutil.parser import parse
from gzip import GzipFile

# components of an Apache logfile line
_LOG_PARTS = [
    r'(?P<host>\S+)',                   # host %h
    r'\S+',                             # indent %l (unused)
    r'(?P<user>\S+)',                   # user %u
    r'\[(?P<timestamp>.+)\]',           # time %t
    r'"(?P<request>.+)"',               # request "%r"
    r'(?P<status>[0-9]+)',              # status %>s
    r'(?P<size>\S+)',                   # size %b (careful, can be '-')
    r'"(?P<referrer>.*)"',              # referrer "%{Referrer}i"
    r'"(?P<user_agent>.*)"',            # user agent "%{User-agent}i"
    r'(?P<request_size>.*)',            # request size
    r'(?P<request_duration>.*)',        # request duration (secs)
    r'"(?P<domain>.*)"',                # site domain
    r'"(?P<app_token>.*)"',             # app token
    r'"(?P<d>.*)"'                      # ?
]

# type conversions for a few columns
TYPE_CONVERTERS = frozendict({
    "timestamp": partial(parse, fuzzy=True),
    "size": int,
    "request_size": int,
    "request_duration": float
})

# declare expected columns and types
COLUMNS = ("host", "user", "timestamp", "request", "status", "size",
           "referrer", "user_agent", "request_size", "request_duration",
           "domain", "app_token", "d", "query")

def _convert_type(k, v):
    return TYPE_CONVERTERS.get(k, lambda x: x)(v)

def _convert_types(data):
    return {k: _convert_type(k, v) for k, v in data.items()}

_REQUEST_RE = re.compile(r"(?:DELETE|POST|GET|PUT)\s+" \
                         r"(?P<path>.*?)\s+" \
                         r".*")

def parse_path(r):
    """Extract an HTTP request path from a raw Nginx request field."""
    m = _REQUEST_RE.search(r)
    return m.groupdict().get("path") if m else ""

# pattern for extracting query terms from request path
_QUERY_STRING_RE = re.compile("q=(.*?)(?:&|$)")

def parse_query(r):
    m = _QUERY_STRING_RE.search(parse_path(r))
    return urllib.unquote_plus(m.group(1)) if m else ""

# pattern composed of capturing subpatterns defined above
_LOG_PARTS_RE = re.compile(r'\s+'.join(_LOG_PARTS) + r'\s*\Z')

def parse_log_line(s):
    """Parse a single line from an Apache logfile."""
    m = _LOG_PARTS_RE.match(s)
    record = _convert_types(m.groupdict()) if m else {}
    if record:
        record.update({"query": parse_query(record["request"])})
    return record

def read_zipped_apache_log_file_as_dict(filename):
    """Read an entire Apache log file into a Pandas DataFrame."""
    with GzipFile(filename) as f:
        return [r for r in (parse_log_line(x) for x in f) if r]

def load_query_logs(*query_files):
    """Read multiple Apache log files into a Pandas DataFrame."""
    return reduce(lambda acc, qf: acc.append(read_zipped_apache_log_file_as_dict(qf)),
                  query_files, pd.DataFrame(columns=COLUMNS))

def _domain_filter(domain):
    domain_lower = domain.lower()
    return not ("rc-socrata.com" in domain_lower or
                "demo.socrata.com" in domain_lower or
                "test-socrata.com" in domain_lower)

def _bot_filter(user_agent):
    ua_lower = user_agent.lower()
    return not ("bot" in ua_lower or "spider" in ua_lower or
                "crawler" in ua_lower or "curl" in ua_lower or
                "ruby" in ua_lower)

def _request_filter(request):
    return "browse" in request and "q=" in request

_FXF_RE = re.compile("^[a-z0-9]{4}-[a-z0-9]{4}$")

def _query_filter(query):
    return not _FXF_RE.match(query, re.IGNORECASE)

def apply_filters(log_record):
    return _domain_filter(log_record["domain"]) and \
        _bot_filter(log_record["user_agent"]) and \
        _request_filter(log_record["request"]) and \
        _query_filter(log_record["query"])

if __name__ == "main":
    from datetime import datetime
    import sys
    import simplejson as json

    class __JSONDateEncoder__(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            return json.JSONEncoder.default(self, obj)

    for line in sys.stdin:
        record = parse_log_line(line.strip())
        if record and apply_filters(record):
            try:
                print json.dumps(record, cls=__JSONDateEncoder__)
            except Exception as e:
                print >> sys.stderr, "{}".format(e.message)

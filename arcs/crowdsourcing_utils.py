import logging
import os
import re
import requests
import simplejson as json
from datetime import datetime
from itertools import chain
from requests import HTTPError
from requests.exceptions import SSLError
from spacy.en import English

CROWDFLOWER_MAX_ROW_LENGTH = 32767
_LOGO_UID_RE = re.compile(r"^[A-Z0-9]{8}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{12}$")

LOGGER = logging.getLogger(__name__)

NLP = English()
SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN")


def get_domain_image(domain):
    """
    Get the site logo for the specified domain.

    Args:
        domain (str): The domain cname

    Returns:
        A URL to the domain's logo
    """
    url = 'http://{0}/api/configurations.json'.format(domain)

    params = {'type': 'site_theme',
              'defaultOnly': True,
              'merge': True}

    response = None

    try:
        response = requests.get(url, params=params, timeout=(5, 10))
        response.encoding = 'utf-8'
        response.raise_for_status()

        data = next((x for x in response.json()[0]["properties"]
                     if "name" in x and x["name"] == "theme_v2b"))

        url = data.get("value", {}).get("images", {}).get("logo_header", {}) \
                                                     .get("href")

        if url and _LOGO_UID_RE.match(url):
            url = "/api/assets/{0}".format(url)

        if not (url.startswith("http") or url.startswith("https")):
            url = "http://{0}{1}".format(domain, url)

        return url

    except IndexError as e:
        print("Unexpected result shape: zero elements in response JSON")
        print("Response: {}".format(response.content if response else None))
        print("Exception: {}".format(e.message))
    except StopIteration as e:
        print("Unable to find image properties in response JSON")
        print("Response: {}".format(response.content if response else None))
        print("Exception: {}".format(e.message))
    except Exception as e:
        print("Failed to fetch configuration for %s" % domain)
        print("Response: %s" % response.content if response else None)
        print("Exception: %s" % e.message)


def gather_columns_types(domain, fxf):
    """
    Gather the column headers and their types

    Args:
        domain (str): A domain cname
        fxf (str): An identifier for a dataset

    Returns:
        A list of pairs of containing a column name and a column type
    """
    url = 'http://{}/api/views/{}/columns.json'.format(domain, fxf)    

    try:
        response = requests.get(url)
    except SSLError:
        LOGGER.error(
            "SSLError occurred while gathering column types for {} from domain {}".format(
                fxf, domain))
        response = None

    column_types = []

    if response:
        try:
            response.raise_for_status()
        except HTTPError:
            response_body = []
        else:
            response_body = response.json()

            if isinstance(response_body, dict) and "error" in response_body:
                response_body = []

        def extract(column):
            return (column.get("name"), column.get("fieldName"), column.get("dataTypeName"))

        column_types = [extract(column) for column in response_body]

    return column_types


def convert_to_table(header, rows):
    """
    Create an HTML table out of the sample data.

    Args:
        header (str): The table header
        rows (List[str]): A list of rows as strings

    Returns:
        A dataset sample in the form of an HTML <table>
    """
    header_html = '<tr><th>{}</th></tr>'.format('</th><th>'.join(header))
    rows_html = ['<tr><td>{}</td></tr>'.format('</td><td>'.join(row)) for row in rows]

    if rows:
        table = '<table>{header}\n{rows}</table>'.format(
            header=header_html, rows='\n'.join(rows_html))
    else:
        table = None

    return table


def _join_sentences(acc, sentences, max_length):
    if len(sentences) > 0 and len(acc + " " + sentences[0]) < max_length:
        return _join_sentences(acc + " " + sentences.pop(0), sentences, max_length)
    else:
        return acc.strip()


def cleanup_description(desc, nlp=None):
    """
    Make a dataset description is readable and not ridiculously long.

    Args:
        desc (str): A dataset (or other core object) description

    Returns:
        Returns a trimmed version of the description, containing as many sentences from the
        description as can fit in a string without exceeding 400 characters
    """
    analyzer = nlp or NLP
    desc = desc.replace("\r", "\n")
    desc_doc = analyzer(desc) if desc else desc
    desc_sentences = [s.text.replace("\n", " ").strip() for s in desc_doc.sents] if desc else []

    return _join_sentences("", desc_sentences, 400) if desc else desc


def extract_address(cell_contents):
    """
    If the cell data is an address, extract the user-friendly bits of it.

    Args:
        cell_contents (Any): The contents of a cell

    Returns:
        A friendly address when one is extractable
    """
    LOGGER.debug("Extracting address from {}".format(cell_contents))

    if "human_address" in cell_contents:
        ad = json.loads(cell_contents["human_address"])
        fields = [ad.get('address'), ad.get('city'), ad.get('state'), ad.get('zip')]
        non_null_fields = [str(f) for f in fields if f]
    elif "latitude" in cell_contents and "longitude" in cell_contents:
        ad = (cell_contents.get("latitude"), cell_contents.get("longitude"))
        if ad[0] and ad[1]:
            non_null_fields = ["(" + ad[0], ad[1] + ")"]
    else:
        non_null_fields = []

    s = ', '.join(non_null_fields)

    return s


def stringify(cell_contents):
    """
    Extract user-friendly strings from cell contents.

    Args:
        cell_contents (Any): The contents of a cell

    Returns:
        The cell contents as a string
    """
    def flatten(d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def is_list_of_dicts(cell_contents):
        return isinstance(cell_contents[0], dict)

    def str_(v):
        return ("%.2f" % v) if isinstance(v, float) else str(v)

    if isinstance(cell_contents, list):
        if is_list_of_dicts(cell_contents):
            strs_to_join = chain.from_iterable([flatten(d).values() for d in cell_contents])
        else:
            strs_to_join = [str_(value) for value in cell_contents if value]

        stringified = ', '.join(strs_to_join)
    else:
        stringified = str(cell_contents)

    return stringified


def _replace_hexadecimal_str(s):
    """
    Replace "\xe8"-type strings with a single hyphen.
    """
    unicodey = r'\\x[abcdef0-9]{2}'

    return re.sub(unicodey, '-', s)


def convert_row(column_name_type, row):
    """
    Make cell data more user friendly.

    Args:
        column_name_type (List[(str, str)]): A list of pairs of column names and types
        row (dict): The row data as a dict

    Returns:
        A list containing row data transformed to be more user-friendly
    """
    row_converted = []

    for column_name, column_type in column_name_type:
        contents = row.get(column_name)
        if not contents or not column_type:
            contents = " "
        else:
            if 'date' in column_type and str(contents).isdigit():
                contents = datetime.fromtimestamp(int(contents)).strftime('%Y-%m-%d')
            elif 'money' in column_type:
                # EVERYBODY IS IN AMERICA RIGHT
                contents = '${}'.format(contents)
            elif 'location' in column_type:
                contents = extract_address(contents)
            else:
                contents = stringify(contents)

        contents = _replace_hexadecimal_str(contents)

        row_converted.append(contents)

    return row_converted


def gather_rows(domain, fxf, columns_names, columns_types, num_rows):
    """
    Gather some sample rows from this dataset to create a short snippet of a dataset.

    Args:
        domain (str): A domain cname
        fxf (str): An identifier for a dataset
        columns_names (List[str]): A list of column names
        columns_types (List[str]): A list of column types
        num_rows (int): The number of rows to gather

    Returns:
        A list of rows representing a sample of rows and columns from a selected dataset
    """
    headers = {"X-App-Token": SOCRATA_APP_TOKEN}

    url = 'https://{}/resource/{}.json?$select={}&$limit={}'.format(
        domain, fxf, ','.join(columns_names), num_rows)

    def get_row_data():
        try:
            response = requests.get(url, headers=headers)
        except SSLError:
            LOGGER.error(
                "SSLError occurred while gathering row data for {} from domain {}".format(
                    fxf, domain))
            response = None

        rowdata = []

        if response:
            try:
                response.raise_for_status()
            except HTTPError:
                rowdata = []
            else:
                response_body = response.json()
                rowdata = [x for x in response_body if x]

        return rowdata

    column_name_type = list(zip(columns_names, columns_types))
    return [convert_row(column_name_type, row) for row in get_row_data()]


def make_dataset_sample(domain_cname, fxf, num_rows, num_columns):
    """
    Make a sample HTML table for a dataset, with `num_rows` rows and `num_columns` columns.

    Args:
        domain_cname (str): A domain cname
        fxf (str): An identifier for a dataset
        num_rows (int): The number of rows to show in the dataset sample
        num_columns (int): The number of columns to show in the dataset sample

    Returns:
        An HTML table as a string
    """
    columns_types = gather_columns_types(domain_cname, fxf)[:num_columns]
    header, columns_names, datatypes = zip(*columns_types) if columns_types else ([], [], [])
    rows = gather_rows(domain_cname, fxf, columns_names, datatypes, num_rows)
    table = convert_to_table(header, rows)
    return table if table and len(table) < CROWDFLOWER_MAX_ROW_LENGTH else None

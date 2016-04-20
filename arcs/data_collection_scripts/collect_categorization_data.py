import re
import csv
import random
import requests
from argparse import ArgumentParser
from datetime import datetime


class ArgHandler(ArgumentParser):
    def __init__(self, **kwargs):
        ArgumentParser.__init__(self, **kwargs)

        self.add_argument('-n', '--num_records',
                          help='Number of records to generate, default %(default)s',
                          type=int,
                          default=100)

        self.add_argument('-d', '--domains',
                          help='A list of particular domains to generate records from',
                          nargs='*')

        self.add_argument('-p', '--num_per_domain',
                          help='Number of records to gather per domain (overrides '
                          '--num_records), default %(default)s',
                          type=int,
                          default=10)

        self.add_argument('-c', '--num_columns',
                          help='Number of columns to limit the preview to, default %(default)s',
                          type=int,
                          default=7)

        self.add_argument('-r', '--num_rows',
                          help='Number of rows to limit the preview to, default %(default)s',
                          type=int,
                          default=5)

        self.add_argument('-o', '--output_file',
                          help='Output file to write records to, defaults '
                          'to a datestamped file in CWD')


def gather_domains(num_records, num_per_domain):
    url = 'http://api.us.socrata.com/api/catalog/domains'

    r = requests.get(url)
    j = r.json()
    domains = j.get('results')
    random.shuffle(domains)

    gathered_domains = []

    # gather 10 extra
    while len(gathered_domains) - 10 < num_records / num_per_domain:
        domain = domains.pop()
        if domain.get('count') < num_per_domain:
            continue
        gathered_domains.append(domain.get('domain'))

    return gathered_domains


def gather_columns_types(domain, fxf, num_columns):
    url = 'http://{}/api/views/{}/columns.json'.format(domain, fxf)

    r = requests.get(url)
    j = r.json()

    columns_types = []

    while len(columns_types) < num_columns and j:
        column = j.pop(0)
        datatype = column.get('dataTypeName')
        name = column.get('name')
        columns_types.append((name, datatype))

    return columns_types


def convert_row(columns_types, row):
    row_types = zip(columns_types, row)

    row_converted = []

    for (name, datatype), contents in row_types:
        if 'date' in datatype:
            contents = datetime.fromtimestamp(contents).strftime('%Y-%m-%d')
        elif 'money' in datatype:
            # EVERYBODY IS IN AMERICA RIGHT
            contents = '${}'.format(contents)
        else:
            contents = str(contents)
        row_converted.append(contents)

    return row_converted


def gather_rows(domain, fxf, columns_types, num_rows, num_columns):
    base_url = 'http://{}/api/views/{}/rows.json?ids={{}}'.format(domain, fxf)

    row_data = []
    for i in xrange(num_rows):
        r = requests.get(base_url.format(i + 1))
        j = r.json()

        rowdata = j.get('data')
        if not rowdata:
            continue

        # the first 8 fields are metadata
        rowdata = rowdata[8:]
        rowdata = rowdata[:num_columns]

        converted_row = convert_row(columns_types, rowdata)

        row_data.append(converted_row)

    return row_data


def convert_to_table(header, rows):
    header_html = '<th>{}</th>'.format('</th><th>'.join(header))
    rows_html = ['<td>{}</td>'.format('</td><td>'.join(row)) for row in rows]

    table = '<table>{header}\n{rows}</table>'.format(header=header_html,
                                                     rows='\n'.join(rows_html))

    return table


def write_csv(header, records, output_file):
    with open(output_file, 'wb') as csvfile:
        writer = csv.writer(csvfile, dialect='excel')

        writer.writerow(header)
        for row in records:
            writer.writerow(row)


_LOGO_UID_RE = re.compile(r"^[A-Z0-9]{8}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{12}$")


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
        print "Unexpected result shape: zero elements in response JSON"
        print "Response: {}".format(response.content if response else None)
        print "Exception: {}".format(e.message)
    except StopIteration as e:
        print "Unable to find image properties in response JSON"
        print "Response: {}".format(response.content if response else None)
        print "Exception: {}".format(e.message)
    except Exception as e:
        print "Failed to fetch configuration for %s" % domain
        print "Response: %s" % response.content if response else None
        print "Exception: %s" % e.message


def gather_records(num_records, domains, num_per_domain, num_rows, num_columns, output_file):
    base_url = 'http://api.us.socrata.com/api/catalog?domains={}'

    gathered_records = []
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
    output_header = ['gathered_on', 'domain', 'fxf', 'name', 'description',
                     'logo_url', 'sample']

    while len(gathered_records) < num_records:
        domain = domains.pop()
        print domain
        logo_url = get_domain_image(domain)

        r = requests.get(base_url.format(domain))
        j = r.json()

        datasets = j.get('results')
        if not datasets:
            continue

        random.shuffle(datasets)

        domain_datasets = []

        while len(domain_datasets) < num_per_domain and datasets:
            dataset = datasets.pop()
            resource = dataset.get('resource')
            if resource.get('type') != 'dataset':
                continue

            name = resource.get('name')
            fxf = resource.get('id')
            description = resource.get('description')

            columns_types = gather_columns_types(domain, fxf, num_columns)

            rows = gather_rows(domain, fxf, columns_types, num_rows, num_columns)
            header = [name for (name, datatype) in columns_types]

            table = convert_to_table(header, rows)

            record = [timestamp, domain, fxf, name, description, logo_url, table]
            domain_datasets.append(record)

        gathered_records.extend(domain_datasets)

    write_csv(output_header, gathered_records, output_file)


if __name__ == '__main__':
    parser = ArgHandler()
    args = parser.parse_args()
    print args

    if not args.domains:
        args.domains = gather_domains(args.num_records, args.num_per_domain)

    print args.domains

    gather_records(args.num_records, args.domains, args.num_per_domain, args.num_rows,
                   args.num_columns, args.output_file)

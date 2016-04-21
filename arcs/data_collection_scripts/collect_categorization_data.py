import json
import re
import csv
import random
import requests
from argparse import ArgumentParser
from datetime import datetime
import sys
reload(sys)
sys.setdefaultencoding("utf-8")


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


def filter_domains(domains):
    valid_domains = []

    for d in domains:
        try:
            requests.get('http://{}/api/configurations.json'.format(d))
            valid_domains.append(d)
        except:
            pass

    return valid_domains


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


def stringify(s):
    if isinstance(s, list):
        if isinstance(s[0], dict):
            # attempt something?
            vals = [entry.values() for entry in s]
            flattened = [str(item) for sublist in vals for item in sublist]
            s = ', '.join(flattened)
        else:
            non_null_fields = [str(f) for f in s if f]
            s = ', '.join(non_null_fields)
    else:
        s = str(s)

    return s


def extract_address(s):
    if isinstance(s[0], basestring) and s[0][0] == '{':
        ad = json.loads(s[0])
        fields = [ad.get('address'), ad.get('city'), ad.get('state'), ad.get('zip')]
        non_null_fields = [str(f) for f in fields if f]
        s = ', '.join(non_null_fields)
    else:
        s = '{}, {}'.format(s[1], s[2])

    return s


def replace_unicode_crap(s):
    # sometimes it's not worth it to backtrack...
    unicodey = r'\\x[abcdef0-9]'

    return re.sub(unicodey, '-', s)


def convert_row(columns_types, row):
    row_types = zip(columns_types, row)

    row_converted = []

    for (name, datatype), contents in row_types:
        if not contents:
            contents = " "
        else:
            if 'date' in datatype and str(contents).isdigit():
                contents = datetime.fromtimestamp(int(contents)).strftime('%Y-%m-%d')
            elif 'money' in datatype:
                # EVERYBODY IS IN AMERICA RIGHT
                contents = '${}'.format(contents)
            elif 'location' in datatype:
                contents = extract_address(contents)
            else:
                contents = stringify(contents)

        contents = replace_unicode_crap(contents)

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

        rowdata = rowdata[0]
        if not rowdata:
            continue

        # the first 8 fields are metadata
        rowdata = rowdata[8:]
        rowdata = rowdata[:num_columns]

        converted_row = convert_row(columns_types, rowdata)

        row_data.append(converted_row)

    return row_data


def convert_to_table(header, rows):
    header_html = '<tr><th>{}</th></tr>'.format('</th><th>'.join(header))
    rows_html = ['<tr><td>{}</td></tr>'.format('</td><td>'.join(row)) for row in rows]

    table = '<table>{header}\n{rows}</table>'.format(header=header_html,
                                                     rows='\n'.join(rows_html))

    return table


def write_csv(header, records, output_file):
    with open(output_file, 'wb') as csvfile:
        writer = csv.writer(csvfile, dialect='excel')

        writer.writerow(header)
        for row in records:
            writer.writerow(row)


def gather_records(num_records, domains, num_per_domain, num_rows, num_columns, output_file):
    base_url = 'http://api.us.socrata.com/api/catalog?domains={}'

    gathered_records = []
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
    output_header = ['gathered_on', 'domain', 'fxf', 'name', 'description',
                     'sample']

    while len(gathered_records) < num_records:
        domain = domains.pop()
        print(domain)

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

            name = replace_unicode_crap(resource.get('name'))
            fxf = resource.get('id')
            description = replace_unicode_crap(
                resource.get('description')).strip() or '[no description]'

            columns_types = gather_columns_types(domain, fxf, num_columns)

            rows = gather_rows(domain, fxf, columns_types, num_rows, num_columns)
            header = [name for (name, datatype) in columns_types]

            table = convert_to_table(header, rows)

            record = [timestamp, domain, fxf, name, description, table]
            domain_datasets.append(record)

        gathered_records.extend(domain_datasets)

    write_csv(output_header, gathered_records, output_file)


if __name__ == '__main__':
    parser = ArgHandler()
    args = parser.parse_args()
    print(args)

    if not args.domains:
        args.domains = gather_domains(args.num_records, args.num_per_domain)

    domains = filter_domains(args.domains)

    if not args.output_file:
        args.output_file = datetime.now().strftime('%Y-%m-%d.%H:%M.csv')

    print(domains)

    gather_records(args.num_records, domains, args.num_per_domain, args.num_rows,
                   args.num_columns, args.output_file)

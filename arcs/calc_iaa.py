import sys
import csv
from krippendorff_alpha import *
from collections import Counter
import pdb


def read_info_top_n(csv_file, n):
    rows = {}
    with open(csv_file) as c:
        reader = csv.reader(c)
        header = reader.next()
        relevance_i = header.index('relevance')
        unit_i = header.index('_unit_id')
        for row in reader:
            rels = row[relevance_i].split()
            unit = int(row[unit_i])
            c = Counter(rels)
            _rels = []
            while len(_rels) < n:
                mc = c.most_common(1)[0][0]
                c.subtract(mc)
                _rels.append(int(mc))
            rows[unit] = _rels

    return rows


def read_info(csv_file):
    rows = {}
    with open(csv_file) as c:
        reader = csv.reader(c)
        header = reader.next()
        relevance_i = header.index('relevance')
        unit_i = header.index('_unit_id')
        for row in reader:
            rels = [int(r) for r in row[relevance_i].split()]
            unit = int(row[unit_i])
            rows[unit] = rels

    return rows


def get_iaa(rows):
    missing = '*'  # indicator for missing values
    print "IAA: %.3f" % krippendorff_alpha(info, preprocessed=True, metric=ordinal_metric, convert_items=int, missing_items=missing)


if __name__ == "__main__":
    info = read_info_top_n(sys.argv[1], 3)
    get_iaa(info)

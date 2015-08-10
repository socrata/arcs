import argparse
from krippendorff_alpha import *
from collections import Counter
import pandas as pd


def make_df(csv_file):
    df = pd.read_csv(csv_file)
    df = df[['relevance']]
    df = df['relevance'].str.split()
    df = df.apply(lambda x: [int(y) for y in x])
    return df


def get_most_common(iterable, n=3):
    # counters are dumb, and don't like being
    # a collection of ints
    c = Counter([str(i) for i in iterable])
    _rels = []
    while len(_rels) < n:
        mc = c.most_common(1)[0][0]
        c.subtract(mc)
        _rels.append(int(mc))
    return _rels


def read_info_top_n(csv_file, n):
    rels = make_df(csv_file)
    rels = rels.apply(get_most_common, n=n)
    return rels.to_dict()


def read_info(csv_file):
    rels = make_df(csv_file)
    return rels.to_dict()


def get_iaa(rows):
    missing = '*'  # indicator for missing values
    print "IAA: %.3f" % krippendorff_alpha(info, preprocessed=True, metric=ordinal_metric, convert_items=int, missing_items=missing)


def arg_parser():
    parser = argparse.ArgumentParser(description='Calculate IAA')

    parser.add_argument('-t', '--top_n', dest='top_n', default=False,
                        action='store_true',
                        help='Whether to get the top n results or use all of them, \
                        default %(default)s')
    parser.add_argument('-n', '--n', dest='n', default=3,
                        help='The n to use if top_n specified, \
                        default %(default)s')
    parser.add_argument('-c', '--csv_file', dest='csv_file', required=True,
                        help='CSV input file to use')

    args = parser.parse_args()
    return args

if __name__ == "__main__":
    args = arg_parser()
    if args.top_n:
        info = read_info_top_n(args.csv_file, args.n)
    else:
        info = read_info(args.csv_file)
    get_iaa(info)

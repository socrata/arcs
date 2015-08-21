import argparse
import requests
import simplejson
from datetime import datetime
import pandas as pd
import numpy as np
from experiment import Group, Experiment
from collection import cleanup_description


def get_dataset_url(domain, _id, name):
    """
    Generate a Google spreadsheets-friendly hyperlink from the dataset's domain,
    fxf, and name.

    Args:
        domain: The domain of the dataset.
        _id: The fxf of the dataset.
        name: The name of the dataset.

    Returns: A hyperlinked dataset name cell, suitable for a Google spreadsheet.
    """
    url = "https://{}/d/{}".format(domain, _id)
    return u'=HYPERLINK("{}";"{}")'.format(url, name)


def gather_results(domain_query, results_per_query, group_name):
    """
    Collect search results from Clytemnestra for each (domain, query) pair
    passed as input.

    Args:
        domain_query: An iterable of (domain, query) pairs.
        results_per_query: The number of results to fetch for each domain, query
           pair.
        group_name: A string identifier for the group of results.

    Returns: A Pandas DataFrame containing the following columns: position,
        domain, query, fxf, name, description, and group.
    """
    all_qrps = []

    for domain, query in domain_query:
        params = {"q": query, "page": 1, "limit": results_per_query,
                  "sortBy": "relevance"}

        r = requests.get("https://{}/api/search/views.json".format(domain),
                         params=params)

        results = r.json().get("results")

        qrps = [(domain, query,
                 x["view"]["id"],  # fxf
                 get_dataset_url(domain, x["view"]["id"], x["view"]["name"]),
                 cleanup_description(x["view"].get("description")), group_name)
                for x in results] if results else []

        # add in result position
        if qrps:
            all_qrps.extend([(result_pos,) + qrp for result_pos, qrp in enumerate(qrps)])
        else:
            print "No results for ({}, {})".format(domain, query)

        if len(qrps) < results_per_query:
            print "Fewer than {} results for ({}, {})".format(results_per_query, domain, query)

    df = pd.DataFrame(all_qrps, columns=("position", "domain", "query", "fxf", "name",
                                         "description", "group"))

    return df


def get_unique_qrps(df):
    """
    Get all unique (domain, query, fxf) triples from a DataFrame of search
    results.

    Args:
        df: A Pandas DataFrame containing at a minimum, the following columns:
            domain, query, and fxf.

    Returns: A Pandas DataFrame containing the same data with any redundant rows
        (by domain, query, fxf) removed.
    """
    return df.groupby(["domain", "query", "fxf"], as_index=False).first()


def shuffle_results(df):
    """
    Shuffle a Pandas DataFrame of search results data. We do this so that
    annotators aren't necessarily judging all results for the same queries.

    Args:
        df: A Pandas DataFrame.

    Returns: A Pandas DataFrame containing the same data, but shuffled.
    """
    return df.iloc[np.random.permutation(len(df))]


def make_csv_for_judgment(csv_filename, baseline_data, grp1_data):
    """
    Create a CSV containing all unique domain QRPs from the two groups that is
    suitable for Google spreadsheet.

    Args:
        csv_filename: A string corresponding to the name of the CSV that will be
            created.
        baseline_data: A Pandas DataFrame containing the baseline system's
            result data. It must have at a minimum the following columns:
            domain, query, name, description, and fxf.
    """
    all_results_df = baseline_data.append(grp1_data)
    unique_qrps_df = get_unique_qrps(all_results_df)
    shuffled_df = shuffle_results(unique_qrps_df)
    shuffled_df = shuffled_df[["domain", "query", "name", "description", "fxf"]]

    # this is necessary because single quotes at the beginning of a CSV cell are
    # mangled by Google spreadsheets
    shuffled_df["query"] = shuffled_df["query"].apply(
        lambda q: "'" + q if q.startswith("'") else q)

    shuffled_df.to_csv(csv_filename, encoding="utf-8", index=False)


def process_judged_results(judged_csv):
    """
    Read a CSV of judged results into a Pandas DataFrame.

    Args:
        judged_csv: A path to the CSV containing the judged data to load.

    Returns: A DataFrame containing the following columns: domain, query, fxf,
        and judgment.
    """
    judged_results_df = pd.read_csv(judged_csv)
    return judged_results_df[["domain", "query", "fxf", "judgment"]]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Gather catalog search results (from Clytemnestra) before "
        "and after tweaking search parameters.")

    parser.add_argument("input_file",
                        help="A 2-column tab-separate file of domains and queries.")

    parser.add_argument("experimental_group_name",
                        help="A string identifier for the experimental group",
                        type=str)

    parser.add_argument("-n", "--num_results_per_query", dest="results_per_query", type=int,
                        default=5,
                        help="Number of results to fetch per query, default %(default)s")

    parser.add_argument("-o", "--output_file", dest="output_file", type=str,
                        default="{}.cly_testing.csv".format(datetime.now().strftime("%Y%m%d")),
                        help="Filename for resulting CSV")

    args = parser.parse_args()

    # load query data
    domain_query = [tuple(x.strip().split('\t')) for x in open(args.input_file)]

    # gather baseline data
    baseline_grp = Group(
        "baseline",
        gather_results(domain_query, args.results_per_query, "baseline"))

    baseline_grp.write_to_csv(baseline_grp)

    raw_input("Waiting to refetch results for system to be updated... Press enter when ready.")

    # gather data from modified system
    grp1 = Group(
        args.experimental_group_name,
        gather_results(domain_query, args.results_per_query, args.experimental_group_name))

    grp1.write_to_csv(grp1)

    # define an experiment
    experiment = Experiment(baseline_grp, grp1)

    make_csv_for_judgment(args.output_file, baseline_grp.data, grp1.data)

    judged_results_file = raw_input(
        "Results have been written to {}. Once they have been judged, please "
        "download the judged CSV, and enter the filename: ".format(args.output_file))

    judged_results_df = process_judged_results(judged_results_file.strip())

    # output experiment stats
    print simplejson.dumps(experiment.stats(judged_results_df), indent=4)

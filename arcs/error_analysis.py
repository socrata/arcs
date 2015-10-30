import pandas as pd


def get_irrelevant_qrps(results):
    """
    Get a list of query-result pairs from the raw judgments where more than one
    worker gave the result a relevance score of 0.
    """
    error = "irrelevant"
    return [(d["query"], d["name"], d["link"], error, d["relevance"]["res"].count("0"))
            for d in results.itervalues() if d["relevance"]["res"].count("0") > 1]


def get_missing_info_qrps(results):
    """
    Get a list of query-result pairs from the raw judgments where one or more
    workers judged the result unjudgeable.
    """
    error = "not enough info"
    return [(d["query"], d["name"], d["link"], error, d["relevance"]["res"].count("-1"))
            for d in results.itervalues() if d["relevance"]["res"].count("-1") > 1]

if __name__ == "__main__":
    import argparse
    from download_crowdflower import get_judgments

    parser = argparse.ArgumentParser(
        description='Gather data from CrowdFlower judgments to server as '
        'the basis for error analysis')

    parser.add_argument('job_id', type=int)
    parser.add_argument('-o', '--outfile', dest='outfile', type=str, required=True,
                        help='Name of CSV file to which data will be written.')

    args = parser.parse_args()

    job_id = args.job_id
    judgments = get_judgments(int(job_id))
    columns = ["query", "name", "link", "error_type", "num_bad_judgments"]

    irrelevant_df = pd.DataFrame.from_records(
        get_irrelevant_qrps(judgments), columns=columns)

    missing_info_df = pd.DataFrame.from_records(
        get_missing_info_qrps(judgments), columns=columns)

    errors_df = irrelevant_df.append(missing_info_df)
    errors_df["num_bad_judgments"] = errors_df["num_bad_judgments"].astype(int)
    errors_df = errors_df.groupby(("query", "name")).agg(lambda x: x.iloc[0])
    errors_df = errors_df.sort("num_bad_judgments", ascending=0)

    outfile = args.outfile or "errors.csv"
    errors_df.to_csv(outfile, encoding="utf-8", cols=columns)

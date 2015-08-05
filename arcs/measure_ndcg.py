from math import log


def compute_ndcg(iterable, acceptable_responses=frozenset([0, 1, 2, 3])):
    """
    :param iterable: the iterable full of judgements
    :param acceptable_responses: a set containing all legal values
    """
    # skip this assert because we're getting averages back
    # assert set(iterable).difference(acceptable_responses) == set(), "Received unexpected response(s) in set {}".format(str(iterable))

    def _calc_dcg(l):
        dcg = l[0]
        for i, n in enumerate(l[1:]):
            dcg += n/log(i+2, 2)
        return dcg

    dcg = _calc_dcg(iterable)
    ideal_order = sorted(iterable, reverse=True)
    idcg = _calc_dcg(ideal_order)
    try:
        ndcg = dcg/idcg
    except Exception:
        print "Uh oh! Error calculating"
        print iterable
        print dcg, "/", idcg
        ndcg = None
    return ndcg


def test():
    lst = [3, 2, 3, 1, 2, 0]
    print compute_ndcg(lst)
    lst = [3, 2, 3, 1, 2, 1]
    print compute_ndcg(lst)
    lst2 = [i-1 for i in lst]
    print compute_ndcg(lst2, set([-1, 0, 2, 1]))


if __name__ == "__main__":
    test()

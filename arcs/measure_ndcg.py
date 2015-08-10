from math import log


def _calc_dcg(judgments, indices):
    return sum([(2**j - 1) / log(i + 2, 2) for (i, j) in zip(indices, judgments)])


def compute_ndcg(iterable, acceptable_responses=frozenset([0, 1, 2, 3]), indices=None):
    """
    :param iterable: the iterable full of judgements
    :param acceptable_responses: a set containing all legal values
    :param indices: the indices of the list, in case we skip a ranking
    """
    if not indices:
        indices = range(len(iterable))
    dcg = _calc_dcg(iterable, indices)
    ideal_order = sorted(iterable, reverse=True)
    idcg = _calc_dcg(ideal_order, indices)
    try:
        ndcg = dcg/idcg
    except Exception:
        print "Uh oh! Error calculating"
        print iterable
        print dcg, "/", idcg
        ndcg = None
    return ndcg


def close_enough(a, b):
    if abs(a-b) < .0000001:
        return True
    return False


def test():
    lst = [3, 2, 3, 1, 2, 0]
    exp = 0.958112357102
    assert close_enough(compute_ndcg(lst), exp), "Uh oh! Expecting {} for {}".format(exp, lst)
    lst = [3, 2, 3, 1, 2, 1]
    exp = 0.959110289196
    assert close_enough(compute_ndcg(lst), exp), "Uh oh! Expecting {} for {}".format(exp, lst)
    lst2 = [i-1 for i in lst]
    exp = 0.947508362289
    assert close_enough(compute_ndcg(lst2, set([-1, 0, 2, 1])), exp), "Uh oh! Expecting {} for {}".format(exp, lst)


if __name__ == "__main__":
    test()

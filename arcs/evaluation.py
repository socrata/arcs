from math import log


def dcg(judgments, indices):
    """
    Compute DCG given an iterable of result positions and judgments for a
    result at each position.

    Note that we use 0-based indexing (hence the i + 2 in the discount term).

    :param indices: an optional list of result positions for each judgment
    :param judgments: an iterable of numeric relevance judgments
    """
    # import pdb
    # pdb.set_trace()
    
    return sum([(2**j - 1) / log(i + 2, 2) for (i, j) in
                zip(indices, judgments)])


def ndcg(judgments, indices=None, ideal_judgments=None):
    """
    Compute NDCG given an iterable of judgments for a result at each position
    in a result set, along with an optional iterable of corresponding result
    position indices, and an iterable of maximum possible judgments at each
    position.

    :param judgments: an iterable of numeric relevance judgments
    :param indices: an optional iterable of result positions for each judgment
    :param ideal_judgments: an optional iterable of max. attainable judgments
    """
    indices = indices if indices is not None else range(len(judgments))

    ideal_judgments = ideal_judgments if ideal_judgments is not None \
        else sorted(judgments, reverse=True)

    return dcg(judgments, indices) / dcg(ideal_judgments, indices)

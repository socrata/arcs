from math import log
from scipy.stats import wilcoxon


def dcg(judgments, indices):
    """
    Compute DCG given an iterable of result positions and judgments for a
    result at each position.

    Note that we use 0-based indexing (hence the i + 2 in the discount term).

    Args:
        judgments: An iterable of numeric relevance judgments
        indices: An optional iterable of result positions for each judgment

    Returns: The discounted cumulative gain as a float.
    """
    return sum([(2**j - 1) / log(i + 2, 2) for (i, j) in
                zip(indices, judgments)])


def ndcg(judgments, indices=None, ideal_judgments=None):
    """
    Compute NDCG given an iterable of judgments for a result at each position
    in a result set, along with an optional iterable of corresponding result
    position indices, and an iterable of maximum possible judgments at each
    position.

    Args:
        judgments: An iterable of numeric relevance judgments
        indices: An optional iterable of result positions for each judgment
        ideal_judgments: An optional iterable of numeric judgments representing
            the maximum attainable judgment at each result position

    Returns: The normalized discounted cumulative gain as a float.
    """
    indices = indices if indices is not None else range(len(judgments))

    ideal_judgments = ideal_judgments if ideal_judgments is not None \
        else sorted(judgments, reverse=True)

    return dcg(judgments, indices) / dcg(ideal_judgments, indices)


def is_statistically_significant(g1_query_dcgs, g2_query_dcgs, alpha=.05):
    """
    Run the paired Wilcoxon signed rank test to determine if the difference
    between the groups is statistically significant.

    Args:
        g1_query_dcgs: An iterable of query DCG scores
        g2_query_dcgs: An iterable of query DCG scores

    Returns: A pair containing a bool indicating whether the difference is
        statistically significant, and the associated p-value.

    References:
        [Wilcoxon signed-rank test](https://en.wikipedia.org/wiki/Wilcoxon_signed-rank_test)
    """
    T, p = wilcoxon(g1_query_dcgs, g2_query_dcgs, zero_method="pratt")
    return (bool(p <= alpha), p)  # converting numpy bool to bool

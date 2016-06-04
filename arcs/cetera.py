import logging
import requests
from frozendict import frozendict

from collect_domain_query_data import lang_filter

LOGGER = logging.getLogger(__name__)


class Query(object):
    @property
    def text(self):
        return self._text

    @classmethod
    def from_query_str(cls, q):
        parts = q.split('=')
        return FacetSearch(parts[0], parts[1]) if len(parts) == 2 else KeywordSearch(q)

    def to_query_params(self):
        pass


class KeywordSearch(Query):
    def __init__(self, text):
        self._text = text

    def to_query_params(self):
        return {"q": self._text}

    def __str__(self):
        return self.text

    __repr__ = __str__


def prepare_facet(facet):
    if facet == "category":
        prepared = "categories"
    else:
        prepared = facet

    return prepared


class FacetSearch(Query):
    def __init__(self, facet, text):
        super(Query, self)
        self._facet = facet
        self._text = text

    @property
    def facet(self):
        return self._facet

    def to_query_params(self):
        return {prepare_facet(self.facet): self.text}

    def __str__(self):
        return "{}={}".format(self.facet, self.text)

    __repr__ = __str__


def get_cetera_results(domain_query_pairs, cetera_host, cetera_port,
                       num_results=10, cetera_params=None,
                       filter_too_few_results=False):
    """
    Get the top n=num_results catalog search results from Cetera for each
    (domain, query) pair in domain_query_pairs.

    Args:
        domain_query_pairs (Iterable[(str, Query)]): An iterable of domain-query pairs
        cetera_host (str): The Cetera hostname
        cetera_port (int): The port on which to make requests to Cetera
        num_results (int): The number of results to fetch for each query
        cetera_params (Dict[Any, Any]): Optional, additional query parameters for Cetera
        filter_too_few_results (bool): Whether to filter queries with too few results

    Returns:
        A list of (domain, Query, result dict) triples
    """
    LOGGER.info("Getting search results from Cetera")

    if cetera_host and cetera_port:
        url = "http://{}:{}/catalog".format(cetera_host, cetera_port)
    else:
        url = "http://api.us.socrata.com/api/catalog"

    cetera_params = cetera_params or {}
    cetera_params.update({"limit": num_results * 2})  # 2x because we're going to langfilter

    params = frozendict(cetera_params)

    def _get_result_list(domain, query):
        if domain and domain != "www.opendatanetwork.com":
            params_ = params.copy(search_context=domain, domains=domain)
        else:
            params_ = params.copy()

        params_ = params_.copy(**query.to_query_params())

        r = requests.get(url, params=params_)

        return [res for res in list(enumerate(r.json().get("results")))
                if lang_filter(res[1]['resource'].get('description'))][:num_results]

    # make Query objects from query strings
    domain_query_pairs_ = ((d, Query.from_query_str(q)) for (d, q) in domain_query_pairs)

    # get results for each domain, query pair
    res = [(d, q, _get_result_list(d, q)) for d, q in domain_query_pairs_]

    # filter for only the (d, q, result_list) tuples that have at least
    # num_results results
    if filter_too_few_results:
        filtered = [(d, q, rl) for d, q, rl in res if len(rl) >= num_results]
        res = filtered

    return res

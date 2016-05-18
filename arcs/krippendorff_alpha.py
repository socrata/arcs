try:
    import numpy
except ImportError:
    numpy = None


##################################################
# Most of this code taken from:
# http://grrrr.org/data/dev/krippendorff_alpha/krippendorff_alpha.py
#
# ==================================================
# Python implementation of Krippendorff's alpha -- inter-rater reliability
#
# (c)2011 Thomas Grill (http://grrrr.org)
# license: http://creativecommons.org/licenses/by-sa/3.0/
#
# Python version >= 2.4 required
# ==================================================
#
# Many things have been renamed for readability.
# The gist of the algorithm was probably from Wikipedia, which has quite
# a nice writeup.
#
# I added the ordinal_metric and intify_floats functionality, as we're
# measuring on an ordinal scale
##################################################


def nominal_metric(a, b):
    return a != b


def interval_metric(a, b):
    return (a-b)**2


def ratio_metric(a, b):
    return ((a-b)/(a+b))**2


def ordinal_metric(a, b):
    """I think this is right?
    See: https://en.wikipedia.org/wiki/Krippendorff%27s_alpha#Difference_functions
    I think we can assume that the name and the rank are the same"""
    a, b = sorted((a, b))
    sub = (a + b)/2
    return sum([x - sub for x in xrange(a, b)])**2


def intify_floats(d):
    """turn eg. 0.66 into 66"""
    return int(float(d)*100)


def krippendorff_alpha(data, metric=interval_metric, preprocessed=False, force_vecmath=False, convert_items=float, missing_items=None):
    '''
    Calculate Krippendorff's alpha (inter-rater reliability):

    data is in the format
    [
        {unit1:value, unit2:value, ...},  # coder 1
        {unit1:value, unit3:value, ...},   # coder 2
        ...                            # more coders
    ]
    or
    it is a sequence of (masked) sequences (list, numpy.array, numpy.ma.array, e.g.)
    with rows corresponding to coders and columns to items

    metric: function calculating the pairwise distance
    force_vecmath: force vector math for custom metrics (numpy required)
    convert_items: function for the type conversion of items (default: float)
    missing_items: indicator for missing items (default: None)
    '''
    # set of constants identifying missing values
    maskitems = set((missing_items,))
    if numpy is not None:
        maskitems.add(numpy.ma.masked_singleton)

    if preprocessed:
        units = data
    else:
        # convert input data to a dict of items
        units = {}
        for d in data:
            try:
                # try if d behaves as a dict
                diter = d.iteritems()
            except AttributeError:
                # array assumed for d
                diter = enumerate(d)

            for idx, rating in diter:
                if rating not in maskitems:
                    existing_ratings = units.get(idx, [])
                    existing_ratings.append(convert_items(rating))
                    units[idx] = existing_ratings

    # get rid of the units with a single/no rating(s)
    units = dict((idx, rating) for idx, rating in units.iteritems() if len(rating) > 1)
    num_judgements = sum(len(pv) for pv in units.itervalues())

    numpy_metric = (numpy is not None) and ((metric in (interval_metric, nominal_metric, ratio_metric)) or force_vecmath)

    Do = 0.
    for grades in units.itervalues():
        if numpy_metric:
            gr = numpy.array(grades)
            Du = sum(numpy.sum(metric(gr, gri)) for gri in gr)
        else:
            Du = sum(metric(gi, gj) for gi in grades for gj in grades)
        Do += Du/float(len(grades)-1)
    Do /= float(num_judgements)

    De = 0.
    for grades in units.itervalues():
        if numpy_metric:
            gr = numpy.array(grades)
            for g2 in units.itervalues():
                De += sum(numpy.sum(metric(gr, gj)) for gj in g2)
        else:
            for g2 in units.itervalues():
                De += sum(metric(gi, gj) for gi in grades for gj in g2)
    De /= float(num_judgements*(num_judgements-1))

    return 1. - Do/De

if __name__ == '__main__':
    print("Example from http://en.wikipedia.org/wiki/Krippendorff's_Alpha")

    data = (
        "*    *    *    *    *    3    4    1    2    1    1    3    3    *    3",  # coder A
        "1    *    2    1    3    3    4    3    *    *    *    *    *    *    *",  # coder B
        "*    *    2    1    3    4    4    *    2    1    1    3    3    *    4",  # coder C
    )

    missing = '*'  # indicator for missing values
    array = [d.split() for d in data]  # convert to 2D list of string items

    print("nominal metric: %.3f" % krippendorff_alpha(array, nominal_metric, missing_items=missing))
    print("interval metric: %.3f" % krippendorff_alpha(array, interval_metric, missing_items=missing))
    print("ratio metric: %.3f" % krippendorff_alpha(array, ratio_metric, missing_items=missing))
    print("ordinal metric: %.3f" % krippendorff_alpha(array, metric=ordinal_metric, convert_items=intify_floats, missing_items=missing))

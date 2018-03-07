import math


def adapt_decay_rate_to_population(n):
    """
    Approximates a value to be used for `rate` argument in
    exponential decay function

    The rate is such that when used in exponential function

        y = (1-rate) ^ x

    value of y approaches zero approximately when x approaches n

    This is very, very rough, brute-force way of fitting that ratio,
    that I came up with after brief thought on lazy Saturday. I bet this
    is not as awesome as something more scientific you can come up with.
    Yes, this is a challenge to you, the reader. :) (DDotsenko)

    n  rate
    11 0.91
    15 0.66666
    20 0.5
    40 0.25
    60 0.166
    80 0.125
    100 0.1
    1,000 0.01
    10,000 0.001
    100,000 0.0001
    10,000,000 0.000..001

    :param n:
    :return:
    """
    assert n > 10, "This formula's answer makes no sense for values at 10 or under"
    return 0.1**(math.log10(n)-1)


def get_decay_proportion(x, rate, decay_floor=0.0):
    """
    Returns a number in the range from 1 to zero, indicating
    remaining portion of whole (range from 1 to 0) to keep given value of x.

    1
    ||
    | \
    |   \
    |    `"-..______
    ----------------
    0              X

    :param x: Number of units.
    :param decay_floor: Proportion lower than which the score will NOT be reduced
    :param rate: Decay rate
    :return:
    """
    # decay_floor=0.15, rate=0.05
    # with above rate + floor, loses about half the score
    # in about 15 units, approaching the floor by about 50th unit
    return decay_floor + (1-decay_floor) * (1-rate) ** x


def get_fade_in_proportion(x, rate, fade_in_ceiling=0.0):
    """
    Returns a number in range between zero and 1 indicating
    what portion of blah to keep given the we are in some spot on
    x axis.

    1
    |     ,_.------
    |   /
    | /
    ||
    ----------------
    0              X

    :param x:
    :param rate:
    :return:
    """
    return 1 - get_decay_proportion(x, rate, fade_in_ceiling)

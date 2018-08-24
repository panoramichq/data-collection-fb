"""
This module is here to runtime-patch problems with Facebooks SDK or API
"""


def patch_facebook_sdk():
    """
    Performs the actual patching of modules where necessary

    Please take care to explain the patching for each item so we know why we
    are doing that specifically
    """

    from facebook_business.adobjects import adset

    # See: https://github.com/facebook/facebook-php-ads-sdk/issues/294
    adset.AdSet.Field.daily_imps = 'daily_imp'

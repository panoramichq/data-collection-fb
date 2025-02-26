from facebook_business.adobjects import (
    ad,
    adaccount,
    adset,
    campaign,
    comment,
    adcreative,
    advideo,
    customaudience,
    page,
    pagepost,
)

from common.enums.entity import Entity
from common.enums.failure_bucket import FailureBucket

FB_ADACCOUNT_MODEL = adaccount.AdAccount
FB_CAMPAIGN_MODEL = campaign.Campaign
FB_ADSET_MODEL = adset.AdSet
FB_AD_MODEL = ad.Ad
FB_AD_CREATIVE_MODEL = adcreative.AdCreative
FB_AD_VIDEO_MODEL = advideo.AdVideo
FB_CUSTOM_AUDIENCE_MODEL = customaudience.CustomAudience
FB_PAGE_MODEL = page.Page
FB_PAGE_POST_MODEL = pagepost.PagePost
FB_COMMENT_MODEL = comment.Comment

FB_MODEL_ENUM_VALUE_MAP = {
    FB_ADACCOUNT_MODEL: (Entity.AdAccount,),
    FB_CAMPAIGN_MODEL: (Entity.Campaign,),
    FB_ADSET_MODEL: (Entity.AdSet,),
    FB_AD_MODEL: (Entity.Ad,),
    FB_AD_CREATIVE_MODEL: (Entity.AdCreative,),
    FB_AD_VIDEO_MODEL: (Entity.AdVideo, Entity.PageVideo),
    FB_CUSTOM_AUDIENCE_MODEL: (Entity.CustomAudience,),
    FB_PAGE_MODEL: (Entity.Page,),
    FB_PAGE_POST_MODEL: (Entity.PagePost, Entity.PagePostPromotable),
    FB_COMMENT_MODEL: (Entity.Comment,),
}

ENUM_VALUE_FB_MODEL_MAP = {}
for Model, values in FB_MODEL_ENUM_VALUE_MAP.items():
    for value in values:
        ENUM_VALUE_FB_MODEL_MAP[value] = Model


def to_fb_model(entity_id: str, entity_type: str, api=None):
    assert entity_type in Entity.ALL

    if entity_type == Entity.AdAccount:
        fbid = f'act_{entity_id}'
    else:
        fbid = entity_id

    return ENUM_VALUE_FB_MODEL_MAP[entity_type](fbid=fbid, api=api)


class JobStatus:
    """
    A class to represent job states (stage ids) for given jobs. Inherit and add
    your arbitrary states.

    The guideline is:

    - use positive numbers for "good" states
    - use negative numbers for "error" states

    You can write the job statues in two ways:

    either simple:

    MyStatus = 123

    or, compound with explicitly stated failure bucket, which is up to the
    task to decide, like this:

    ErrorStatus = -123, FailureBucket.Throttling


    You do not need to state FailureBucket.Other, as that is assumed to be the
    general case
    """

    # Any job considered started is represented by 100
    Start = 100
    # Any job considered done is represented by 1000
    Done = 1000

    # Any generic failure is represented by -1000 (negative Done)
    GenericError = -1000


class ExternalPlatformJobStatus(JobStatus):
    """
    Use this to communicate to give status reporter enough information to
    figure out what the stage id means in terms of failures
    """

    # Progress states
    DataFetched = 200
    InColdStore = 500

    # Indicates FB hinting us that API call failed because we might have asked for too much data
    TooMuchData = -500
    InaccessibleObject = -550

    AdAccountThrottlingError = -600
    UserThrottlingError = -700
    ApplicationThrottlingError = -800

    GenericPlatformError = -900

    failure_bucket_map = {
        DataFetched: FailureBucket.WorkingOnIt,
        JobStatus.Done: FailureBucket.Success,
        TooMuchData: FailureBucket.TooLarge,
        AdAccountThrottlingError: FailureBucket.AdAccountThrottling,
        UserThrottlingError: FailureBucket.UserThrottling,
        ApplicationThrottlingError: FailureBucket.ApplicationThrottling,
        GenericPlatformError: FailureBucket.Other,
        JobStatus.GenericError: FailureBucket.Other,
        InaccessibleObject: FailureBucket.InaccessibleObject,
    }


class ReportEntityApiKind:

    Ad = 'ads'
    Page = 'page'
    Video = 'video'
    Post = 'post'


class ColdStoreBucketType:
    ORIGINAL_BUCKET = 'orig'
    RAW_BUCKET = 'raw'

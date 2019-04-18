class Entity:

    # Scope within which a collection of some assets (AdAccounts etc)
    # is managed. Example scope value "Console" - identifies Operam Console as
    # source of some assets collection
    Scope = 'Scope'

    AdAccount = 'AA'
    Campaign = 'C'
    AdSet = 'AS'
    Ad = 'A'

    AdCreative = 'AC'
    AdVideo = 'AV'
    CustomAudience = 'CA'

    Page = 'P'
    PagePost = 'PP'
    PagePostPromotable = 'PP_P'
    Comment = 'CM'
    PageVideo = 'PV'

    # FIXME: split entities into general (C,AS,A) - insights enabled
    # FIXME: and into "entity only" - other (no insights fetched)

    AA_SCOPED = {AdAccount, Campaign, AdSet, Ad, AdCreative, AdVideo, CustomAudience}

    NON_AA_SCOPED = {Page, PagePost, Comment, PageVideo, PagePostPromotable}

    ALL = AA_SCOPED.union({Scope}, NON_AA_SCOPED)

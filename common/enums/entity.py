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
    Comment = 'CM'

    # FIXME: split entities into general (C,AS,A) - insights enabled
    # FIXME: and into "entity only" - other (no insights fetched)

    ENTITIES_AA = {
        AdAccount,
        Campaign,
        AdSet,
        Ad,
        AdCreative,
        AdVideo,
        CustomAudience,
    }

    NON_AA = {
        Page,
        PagePost,
        Comment,
    }

    ALL = ENTITIES_AA.union({Scope}, ENTITIES_NON_AA)

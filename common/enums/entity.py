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

    # FIXME: split entities into general (C,AS,A) - insights enabled
    # FIXME: and into "entity only" - other (no insights fetched)

    ALL = {
        AdAccount,
        Campaign,
        AdSet,
        Ad,
        AdCreative,
        AdVideo,
        CustomAudience,
        Scope,
        Page,
        PagePost
    }

    NON_AA = {
        Page,
        PagePost
    }

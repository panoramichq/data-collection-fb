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
    PageVideo = 'PV'

    # FIXME: split entities into general (C,AS,A) - insights enabled
    # FIXME: and into "entity only" - other (no insights fetched)

    AA_SCOPED = {AdAccount, Campaign, AdSet, Ad, AdCreative, AdVideo, CustomAudience}

    NON_AA_SCOPED = {Page, PagePost, Comment, PageVideo}

    ALL = AA_SCOPED.union({Scope}, NON_AA_SCOPED)

    @staticmethod
    def next_level(entity_type):
        if entity_type not in {Entity.AdAccount, Entity.Campaign, Entity.AdSet}:
            raise NotImplementedError(f'Determining next level from type: {entity_type} is not supported')
        return {Entity.AdAccount: Entity.Campaign, Entity.Campaign: Entity.AdSet, Entity.AdSet: Entity.Ad}[entity_type]

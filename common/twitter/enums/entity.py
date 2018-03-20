class Entity:

    # Scope within which a collection of some assets (AdAccounts etc)
    # is managed. Example scope value "Console" - identifies Operam Console as
    # source of some assets collection
    Scope = 'Scope'

    AdAccount = 'AA'
    Campaign = 'C'
    LineItem = 'LI'
    PromotedTweet = 'PT'

    ALL = {
        AdAccount,
        Campaign,
        LineItem,
        PromotedTweet,
    }

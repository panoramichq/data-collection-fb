class Entity:

    # Scope within which a collection of some assets (AdAccounts etc)
    # is managed. Example scope value "Console" - identifies Operam Console as
    # source of some assets collection
    Scope = 'Scope'

    AdAccount = 'AA'
    Campaign = 'C'
    AdSet = 'AS'
    Ad = 'A'

    ALL = {
        AdAccount,
        Campaign,
        AdSet,
        Ad,
        Scope
    }

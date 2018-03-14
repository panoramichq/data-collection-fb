from common.tokens import TokenManager
from sweep_builder.reality_inferrer.adaccounts import iter_scopes


def init_tokens(sweep_id):
    for scope_record in iter_scopes():
        TokenManager(scope_record.scope, sweep_id).add([scope_record.scope_api_token])

        TokenManager('fb', sweep_id).add(scope_record.platform_tokens)

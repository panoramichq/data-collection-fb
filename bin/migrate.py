# temporary place for DB schema application
# Dynamo does not really have "migrate" more like "set"
# Running this again and again should be fine (in testing with brute_force=True)
# Long term, this should do proper upsert of schema in prod too.
import sys
sys.path.insert(0, '.')


def do_it_all():
    from common.store.sync_schema import sync_schema
    sync_schema(brute_force=True)

    # Eventually this should go away and be replaced
    # by direct pull of AdAccount-to-tokens pairings from
    # some sort of Platform assets API
    # Since we anticipate only one Source of AdAccounts - Console
    # and since we use one and same token, just injecting
    # the thing into DB to act as seed scope for iteration over AdAccounts
    # per given token (hardcoded / passed through configs)


    from common.store.scope import FacebookAdAccountScope, FacebookToken, DEFAULT_SCOPE
    token_id = DEFAULT_SCOPE
    FacebookToken.upsert(token_id, token='bogus_token')
    FacebookAdAccountScope.upsert(DEFAULT_SCOPE, token_ids={token_id})


if __name__ == '__main__':
    do_it_all()

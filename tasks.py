# This is Invoke [http://docs.pyinvoke.org/en/latest/] config file
# (it's like Makefile but understand Python and can import your code)
# Do `invoke --list` to see possible commands
# Used mostly easing scaffolding in command line.
# Nothing in production should be using this. Feel free to change in development.


from common.patch import patch_event_loop
patch_event_loop()

from invoke import task
import json

from common.store.scope import AssetScope, PlatformToken, DEFAULT_SCOPE
from tests.base.random import gen_string_id
from common.store.entities import AdAccountEntity
from config.facebook import TOKEN, AD_ACCOUNT, AD_ACCOUNT_TIME_ZONE


@task
def scope_list(ctx):
    for scope in AssetScope.scan():
        for token_id in scope.platform_token_ids:
            pt = PlatformToken.get(token_id)
            print(scope.scope, pt.token)

@task
def scope_set(ctx, scope, token):
    PlatformToken.upsert(scope, token=token)
    AssetScope.upsert(scope, platform_token_ids={scope})


@task
def ad_account_list(ctx):
    for aa in AdAccountEntity.scan():
        print(aa)

@task
def ad_account_set(ctx, scope, id=None,  name='AdAccount', is_active=True, data=None):
    # PlatformToken.upsert(scope, token=TOKEN)
    # AssetScope.upsert(scope, platform_token_ids={scope})

    if data:
        data = json.loads(data)
    else:
        data = {}

    a = AdAccountEntity.upsert(
        scope,
        gen_string_id() if id is None else id,
        is_active=is_active,
        **data
    )
    print(a.to_dict())


@task
def ad_account_delete(ctx, scope, id, complain=False):
    """
    :param ctx:
    :param scope:
    :param id:  is "*" deletes them all
    :param complain:
    :return:
    """
    if id == '*':
        for aa in AdAccountEntity.query(scope):
            aa.delete()

    if complain:
        AdAccountEntity.get(scope, id).delete()
    else:
        AdAccountEntity(scope, id).delete()


@task
def ad_account_remote_view(cts, scope, id, token=None):
    from oozer.common.facebook_api import PlatformApiContext, get_default_fields
    from common.enums.entity import Entity

    if not token:
        scope = AssetScope.get(scope)
        token = PlatformToken.get(list(scope.platform_token_ids)[0])

    with PlatformApiContext(token.token) as fb_ctx:
        ad_account = fb_ctx.to_fb_model(id, Entity.AdAccount)
        fields = get_default_fields(ad_account.__class__)
        ad_account_with_selected_fields = ad_account.api_get(fields=['id','name'])  # Read just the fields we need
        ad_account_data_dict = ad_account_with_selected_fields.export_all_data()  # Export the object to a dict
        print(ad_account_data_dict)


@task
def sweep_run(ctx):
    from oozer.full_loop import run_sweep
    run_sweep()

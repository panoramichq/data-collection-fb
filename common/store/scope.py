from pynamodb import attributes

from common.memoize import memoized_property, MemoizeMixin
from config import dynamodb as dynamodb_config
from config import operam_console_api as operam_console_api_config

from common.store.base import BaseMeta, BaseModel

# at least one value is meaningful here. It indicates
# both, the source of ad accounts AND one-and-only token
# associated with them all
# In this case "console" means "we take ad account IDs
# from Console DB and use that single system user token
# stored in Console for them all"
DEFAULT_SCOPE = 'Console'

# With time ^ this should go away and be replaced
# by proper tokens-to-AdAccount management API


class PlatformToken(BaseModel):
    """
    At this time we have no corporate API for
    (a) management of platform user token assets and linking them to FB entities
    (b) proxying requests to platform over some corporate platform proxy API
        (which would remove the need for passing actual tokens around here)

    When these structural parts are put in,
    remove this table and migrate code to rely on other sources of token data
    """

    Meta = BaseMeta(dynamodb_config.TOKEN_TABLE)

    token_id = attributes.UnicodeAttribute(hash_key=True, attr_name='tid')
    token = attributes.UnicodeAttribute(attr_name='t')


class AssetScope(BaseModel, MemoizeMixin):
    """
    Stores metadata specific to "Scope" that acts as grouping element
    for a collection of attached top-level assets like Ad Accounts

    This is approximately mapped to a Facebook User (token) or some
    cohesive source of Ad Accounts.

    Initially used for tracking / managing the per-sweep sync of Ad Account IDs from
    Console into our internal store for later iteration over that collection.
    """

    Meta = BaseMeta(dynamodb_config.AD_ACCOUNT_SCOPE_TABLE)

    # scope is an ephemeral scoping element
    # Imagine "operam business manager system user" being one of the scope's values.
    # This is here mostly just to simplify iterating
    # through AAs per given known source.
    # (even if originally we will have only one scope in entire system)
    # At first, scope will be pegged one-to-one to
    # one token to be used for all AAs,
    # (Later this relationship may need to be inverted to
    # allow multiple tokens per AA)
    scope = attributes.UnicodeAttribute(hash_key=True, attr_name='scope')

    # TODO: move storage of scope's API tokens, credentials to this model
    # scope_api_credentials = Blah()
    # while we have only one scope - Console - we can fake it
    # by sending same exact token for all (ahem.. one and only) models
    # This obviously needs to rethought at some point.
    # (Part of the rethinking is that this record has to match a
    #  particular partner API implementation that matches this scope
    #  It's crazy to expect that AUTH will be exactly same and that it will
    #  be just by token.)
    scope_api_token = operam_console_api_config.TOKEN

    platform_token_ids = attributes.UnicodeSetAttribute(default=set, attr_name='tids')

    # Memoized in instance to avoid hitting DB all the time we are called.
    @property
    @memoized_property
    def platform_tokens(self):
        """
        Returns a set of actual FB tokens that token_ids attribute point to by ids
        :return:
        """
        return {record.token for record in PlatformToken.scan(PlatformToken.token_id.is_in(*self.platform_token_ids))}

    @property
    def platform_token(self):
        try:
            return list(self.platform_tokens)[0]
        except IndexError:
            return None

    @classmethod
    def clone_from(cls, other: 'AssetScope', **attrs) -> 'AssetScope':
        new_attributes = {'scope': other.scope, 'platform_token_ids': other.platform_token_ids, **attrs}

        return AssetScope(**new_attributes)


def sync_schema(brute_force: bool = False):
    """
    In order to push fidelity and maintenance of table "migrations"
    closer to the code where the models are migrated, this is where
    we'll hook up generically-reused API for upserting our tables.
    Call this from some centralized `sync_schema` script
    """
    from pynamodb.exceptions import TableError, TableDoesNotExist

    tables = [AssetScope, PlatformToken]

    for table in tables:
        # create_table does NOTHING if table already exists - bad
        # when we add keys in the model, nothing will happen below.
        # TODO: adapt this to update as well
        # until then, every time we need to update the tables, just delete
        # them all.
        if brute_force:
            try:
                table.delete_table()
            except (TableError, TableDoesNotExist):
                pass
        table.create_table()

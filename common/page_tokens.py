import logging
from typing import List, Optional

from facebook_business.api import FacebookRequest

from common.connect.redis import get_redis
from common.enums.entity import Entity
from common.store.scope import AssetScope
from oozer.common.facebook_api import PlatformApiContext, DEFAULT_PAGE_ACCESS_TOKEN_LIMIT
from oozer.common.job_scope import JobScope

logger = logging.getLogger(__name__)


class PageTokenManager:
    def __init__(self, asset_scope: str, sweep_id: str):
        self._redis = get_redis()
        self._sweep_id = sweep_id
        self._asset_scope = asset_scope

    def token_queue_key(self, page_id: str) -> str:
        return f'{self._asset_scope}-{self._sweep_id}-page-{page_id}-tokens-queue'

    @classmethod
    def populate_from_scope_entity(cls, scope_entity: AssetScope, sweep_id: str):
        asset_scope = JobScope.namespace
        tokens = list(scope_entity.platform_tokens)

        try:
            manager = PageTokenManager(asset_scope, sweep_id)
            with PlatformApiContext(tokens[0]) as fb_ctx:
                request = FacebookRequest(
                    node_id='me', method='GET', endpoint='/accounts', api=fb_ctx.api, api_type='NODE'
                )
                request.add_params({'limit': DEFAULT_PAGE_ACCESS_TOKEN_LIMIT})
                cnt = 0
                while True:
                    # I assume that there's a better way to do paginate over this,
                    # but I wasn't able to find the corresponding target class in SDK :/
                    response = request.execute()
                    response_json = response.json()
                    for page in response_json['data']:
                        manager.add(page['id'], [page['access_token']])
                        cnt += 1

                    if 'next' in response_json['paging']:
                        request._path = response_json['paging']['next']
                    else:
                        break

                logger.warning(f'Loaded {cnt} page tokens for scope "{scope_entity.scope}"')
        except Exception as ex:
            print(ex)
            logger.warning('Fetching page tokens has failed so organic data jobs will not work in this sweep')

    @classmethod
    def from_job_scope(cls, job_scope: JobScope) -> 'PageTokenManager':
        """
        infers required asset scope parameters from JobScope data
        and creates an instance of PlatformTokenManager properly set for
        management of tokens required for the job.

        Convenience method for use in worker code for quick derivation
        of appropriate scope for the PlatformTokenManager

        This is the "read" side of the "write" side depicted in
        populate_from_scope_entity method immediately above.
        If you change it here, change it there.
        """
        if job_scope.entity_type == Entity.Scope:
            asset_scope = job_scope.entity_id
        else:
            # TODO: This needs to be scope ID somehow eventually
            # as platform tokens are grouped per scope ID
            # Until then, we, effectively, will have only one tokens pool
            asset_scope = job_scope.namespace  # likely something like 'fb' or 'tw'

        return PageTokenManager(asset_scope, job_scope.sweep_id)

    def add(self, page_id: str, *tokens: List[str]):
        """
        Add one or more tokens to the tokens inventory.

        Seeds temporary tokens inventory with tokens, while resetting
        their usage counters to zero.
        """
        self._redis.zadd(
            self.token_queue_key(page_id),
            # switching this from **dict((token, 0) for token in tokens)
            # to positional args list and thus *args passing because when we do dict()
            # each token value becomes a hash key, that is coerced into
            # acting as a named arg, which is dangerous for tokens
            # that may contain characters not allowed to be in variable names.
            # So, keeping them as positional str args instead
            # Combined list must be a sequence of key, score, key2, score2, ...
            *(arg for token in tokens for arg in [token, 0]),
        )

    def remove(self, page_id: str, *tokens: List[str]):
        """
        Like .add but in reverse.
        """
        self._redis.zrem(self.token_queue_key(page_id), *tokens)

    def get_best_token(self, page_id: str) -> Optional[str]:
        token_candidate = (self._redis.zrange(self.token_queue_key(page_id), 0, 1) or [None])[0]
        if token_candidate is not None:
            return token_candidate.decode('utf8')
        return None

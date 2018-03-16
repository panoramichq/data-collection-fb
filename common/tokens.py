from itertools import zip_longest
from typing import List, Set, Union, Optional

import config.application

from common.connect.redis import get_redis
from common.enums.entity import Entity
from common.enums.failure_bucket import FailureBucket
from common.enums.reporttype import ReportType
from oozer.common.job_scope import JobScope


failure_bucket_count_map = {
    # this is the only one that is really deriving any "clever" value from TokenManager
    # this effectively pushes out the use of this token a bit
    FailureBucket.Throttling: 200,
    # If we are getting to "too large" territory, there is a chance
    # that we are hitting costly APIs and our counter will hit throttling soon
    FailureBucket.TooLarge: 5,
}


class PlatformTokenManager:

    def __init__(self, asset_scope, sweep_id):
        self.queue_key = f'{asset_scope}-{sweep_id}-sorted-token-queue'
        self._redis = get_redis()

    @classmethod
    def populate_from_scope_entity(cls, scope_entity, sweep_id):
        """
        Helps dump tokens of various kinds to Redis for use
        by `.from_job_scope` method declared below.

        In the jobs persister we purposefully avoid persisting
        anything besides the Job ID. This means that things like tokens
        and other data on *Claim is lost.
        As long as we are doing that, we need to leave tokens somewhere
        for workers to pick up.

        This is the "write" side of the "read" side that is in `from_job_scope`

        If you change it here, change it there too.

        Although this code does not really fit this place, it's here for
        single super important reason - to keep the code of "write" and
        "read" sides right next to each other to ensure they are in sync.
        Otherwise, yeah, this feels like it should be elsewhere.

        :param common.store.scope.AssetScope scope_entity:
        :param sweep_id:
        :return:
        """

        # for Scope-centric refresh jobs, entity_id element is the Scope ID
        asset_scope = scope_entity.scope
        tokens = [scope_entity.scope_api_token]

        PlatformTokenManager(
            asset_scope,
            sweep_id
        ).add(*tokens)

        # for FB-centric refresh jobs, job namespace is default value on JobScope.namespace
        # and tokens are one or more platform tokens from Scope object
        asset_scope = JobScope.namespace
        tokens = scope_entity.platform_tokens

        PlatformTokenManager(
            asset_scope,
            sweep_id
        ).add(*tokens)

    @classmethod
    def from_job_scope(cls, job_scope):
        """
        infers required asset scope parameters from JobScope data
        and creates an instance of PlatformTokenManager properly set for
        management of tokens required for the job.

        Convenience method for use in worker code for quick derivation
        of appropriate scope for the PlatformTokenManager

        This is the "read" side of the "write" side depicted in
        populate_from_scope_entity method immediately above.
        If you change it here, change it there.

        :param JobScope job_scope:
        :rtype: PlatformTokenManager
        """

        # while we have support for many many scopes,
        # here we temporarily collapse all
        # `fb` namespace jobs into one, single token pool
        # TODO: when Scope ID is on JobScope object, change this to act per Scope ID

        # for Scope-specific jobs,
        #  parent_id is absent,
        #  namespace is set to config.application.UNIVERSAL_ID_SYSTEM_NAMESPACE
        #  entity_id element is the Scope ID
        #  entity_type is Entity.Scope

        if job_scope.entity_type == Entity.Scope:
            asset_scope = job_scope.entity_id
        else:
            # TODO: This needs to be scope ID somehow eventually
            # as platform tokens are grouped per scope ID
            # Until then, we, effectively, will have only one tokens pool
            asset_scope = job_scope.namespace  # likely something like 'fb' or 'tw'

        return PlatformTokenManager(
            asset_scope,
            job_scope.sweep_id
        )

    def add(self, *tokens):
        """
        Add one or more tokens to the tokens inventory.

        Seeds temporary tokens inventory with tokens, while resetting
        their usage counters to zero.

        :param tokens: List (or any other iterable
        :type tokens: List[str]
        :return:
        """
        self._redis.zadd(
            self.queue_key,
            # switching this from **dict((token, 0) for token in tokens)
            # to positional args list and thus *args passing because when we do dict()
            # each token value becomes a hash key, that is coerced into
            # acting as a named arg, which is dangerous for tokens
            # that may contain characters not allowed to be in variable names.
            # So, keeping them as positional str args instead
            # Combined list must be a sequence of key, score, key2, score2, ...
            *(
                arg
                for token in tokens
                for arg in [token, 0]
            )
        )

    def remove(self, *tokens):
        """
        Like .add but in reverse.

        :param tokens:
        :return:
        """
        self._redis.zrem(
            self.queue_key,
            *tokens
        )

    def get_best_token(self):
        # type: () -> Union[str,None]
        token_candidate = (self._redis.zrange(self.queue_key, 0, 1) or [None])[0]
        if token_candidate is not None:
            return token_candidate.decode('utf8')
        return None

    def get_token_count(self):
        # type: () -> int
        return self._redis.zcount(self.queue_key, '-inf', '+inf') or 0

    def report_usage(self, token, usage_count=1):
        """
        Notes somewhere, where-ever we are keeping the token use counts,
        that a given token was used.

        Optionally allows to communicate in one call how many times the token
        was used. By default, it's one.

        :param token:
        :param usage_count:
        :return:
        """
        # type: (str, Optional[int]) -> None
        self._redis.zincrby(self.queue_key, token, usage_count)

    def report_usage_per_failure_bucket(self, token, failure_bucket):
        self.report_usage(
            token,
            failure_bucket_count_map.get(failure_bucket, 1)
        )

from common.tokens import PlatformTokenManager
from sweep_builder.reality_inferrer.adaccounts import iter_scopes


def init_tokens(sweep_id):
    """
    This is an accidental child of two parents: indecision and imperfection.

    *We need tokens in workers.*

    (A) "Clean" way to pass tokens to workers would be
    in the args - celery task args.

    (B) To get there, the cleanest way to have
    tokens available for insertion into celery task call args, is to have that data
    available in the Oozer, for each JobID it pulls from the queue, it also, somehow
    gets the other parts of JobScope data, like tokens, but these are NOT on job ID it pulls.

    (C) So, To have this JobScope additional data available on a per JobID basis to Oozer's looper
    it would be "clean" to pre-apportion (store somewhere temporarily one per each Job ID by
    Sweep Builder.

    (D) In that above case - where Builder just pre-assigns tokens etc scope data to each and single
    data and persists it along each Job ID - our generator pipeline cleanly delivers this stuff
    from Reality inferer, through Expecation Builder, through Prioritizer to Persister - all on top
    of *Claim family of objects - all in process - all fine.... Right?

    Well....

    While effective duplication of Job Scope data inside the Reality>Expectation>Prioritizer chain
    mentioned in part (D) is super cheap duplication - we send exactly same 2-3 tokens with each
    Reality>Expecation>Prioritization Claim objects again and again, we never hold ALL of Claims
    for the sweep in memory. It's like we are watching a movies where parts of the shot repeat
    because they are part of the same stage / scene, but we hold only one frame of the movie at the time.
    We don't feel the weight of data (tokens) duplication at stage (D)

    However, at stage (C) - Sweep Builder *Persister* we are serializing entire movie - all frames,
    all shots, into Redis. Naturally, it feels icky to write same exact 3 tokens several million times
    to different keys in Redis.

    Then, you take this to Oozer level, where all of these million records with same values need to be read,
    serialized into Celery call signatures and again written to Celery broker DB (redis again).
    Luckily there we *ooze* the tasks out, so only several hundred thousand times duplication at the same time.

    All that writing + reading counted in millions + pressure on Redis memory is starting to be annoying.

    And.... then we arrive to realization that the "clean" way of approtioning tokens ahead of time is
    actually not as clean, because we actually need to delay apportioning the tokens until the very end
    when actual worker wakes up from the queue and says "I am ready to do shit!! Which token is still alive?"

    What we realized is that apportioning tokens to each job in builder is completely inefficient,
    that apportioning tokens to each Celery task in Oozer looper is largely inefficient (because it
    shoves thousands of tasks into Celery queue and by then it's late to react to throttling). What
    we realized is that Celery task itself is best picking a token at the time it gets a turn to run.

    So, if workers pick from a weighted / scored collection of platform tokens, awesome shit! Clever!
    But where do we form that collection?....

    This is the piece of shit code where we front-run entire Sweep Builder run and manufacture
    (dump to Redis) collections of tokens (grouped by various scopes). It's here
    because we did not find a better place for it yet.

    TODO: Find a better place for it
          as this off-on-the-side band-aid looper code feels redundant since we have a loop already
    """
    for scope_record in iter_scopes():
        PlatformTokenManager.populate_from_scope_entity(scope_record, sweep_id)

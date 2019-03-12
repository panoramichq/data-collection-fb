"""
Tables in here try to figure out if we collected data for
all possible children for all possible data types.

Some tables here - JobReport - deal with staleness of data.
Others deal with explaining the gaps in data (matching expectations to delivery)

What these tables are trying to help with is answer if
a lack of data (lack of record of us successfully collecting data) is due to
that piece of data not existing anywhere, or due to our failure to
or lack of trying to collect it.

There is no clean way to design a full-proof "we are really done" way of accounting
for work in a distributed, system as long as we collect data using opportunity-weight-based
system that does not care about completing "full cycle per parent" in one go.
There is also no clean way to design "I will complete full cycles per parent" systems
that are agile against throttling and FB "report size" errors.

So, my challenge to you is to decide *if*:

 "I must know when we *really* finished sync for report type for
  given data type per given scope"

is a *sane* or *useful* goal to have.

This system continuously races towards eventual consistency with Facebook's data,
never to be exactly matching it, but striving to be as close as FB allows us,
to build best composite picture of what's in FB.

Lacking a clear way of asking from Facebook what "really done" may mean,
and having no obvious way to infer from errors if you are NOT done
(because there are several ways of to get to same data and errors in one approach
do NOT mean complete failure to get that data), the simplest way to reason
about this in sane way is to assume that:

 *We are Always and Continuously in the best achievable form of Done!*

This means that "at this time, we have the best data we can have at this time" so use it.
In the close future we may get closer to or further away from parity to something that
is always moving away from us.

What these tables play on is on redefining "done" from "really done" to
"*inferred* done".

If you change it from

    "I looked everywhere and assembled *absolutely everything* I can and
    there is only one way of collecting any single piece of data and when that way failed for me
    I marked it clearly as failed, so now I know I am 'Done' in one large loop"

to:

    We have a system that independently, based on whatever constantly improving data
    tries to *infer* where data may be. We have a separate system that tries to use
    many means (some of which fail, but are backed up by other ways) to obtain
    data from those inferred places and records only *successes*
    ("successfully checked there - nothing there" or "successfully checked there -
     got some data") in a log.
    If you, the reader, compare our inferred universe with records of successes,
    you may approximate how "Done" we are.
    System will do the same ^ compare inferred universe with record of prior travel
    and will try to figure out if next short trip is best spent on one sector of
    never-visited or long-unvisited universe.

    Instead of *knowing* we are "done" we are constantly inferring gradation of
    "done-ness" per importance of given sector of universe and focusing our
    scarce resources on sector with lightest shade of "done-ness"

    The shade of "done-ness" may get very close to otherwise unmeasurable "real done"
    but is not guaranteed to do so, only because our approximation of
    "inferred done" is inferred based on potentially imperfect input data.

So, tables below contain our *guess* about what combinations of
per-AdAccountID-per-report_type-per-date may have Entity IDs
that *may* have data. Next to these guesses we leave notes about
our last attempts to verify that guess somehow.

Some other code is responsible for interpreting these records in aggregate
(per AdAccount ID) and communicating gradation of "inferred done-ness" per AdAccount
to rest of the company.

Example of how data may be collected:

    If a per-AdAccount insights report is routinely failing on some page 7 of results
    and we have to switch to per-entity_id insights route. But how do you know
    reliably that given entity_id had insights for that particular day?
    The only way to know is to ask for that. Then imagine 15,000 ads per ad account
    times 700 days of possible inferred lifetime of these ads
    (how do you know when ad really ran without asking for reports?)
    times report variants (DMA, Hourly, Lifetime etc)... ~105,000,000 hits to FB
    just for Ad level just for one AdAccount - all to ensure that there is nothing
    there for every single ad-per-day-per-report type permutation.

    What this system tries to do is to form those 105,000,000 expectations per
    from visible data and record them in this table.

    Then some other parts of the system is marking which particular expectations
    we attempted to fill in which way.

    The most interesting challenge for the system is to design efficient ways
    to determine / guess if there "nothing there to collect" based on clever
    combinations of per-parent-per-day data paging and, if we did not die
    from throttling or other errors mid-flight, loop through other items we had
    expectations for and mark them as "nothing is there" In that lucky,
    error-less outcome, we get everything there is per parent, and can
    quickly mark large ranges of unmet expectations as "checked, nothing there"
    without ever asking for data for those particular Entity IDs.

    In the scenario where per-parent-per-day paging fetch always breaks at some point,
    system absolutely must switch to per-entity_id insights per day fetch.
    In that case, the assumption of "we are done" is only as good as
    our skill of forming the expectation about what Entity IDs *may* have data.
    If we are using local Per-AA Entities data to infer what entity IDs to collect for,
    and there is some report out there for long deleted Entity IDs (that we don't know
    about now), we will never collect insights for those Entity IDs,
    because they will NOT be on the per-AA entities list in our system.
    Deleted children still count towards insights on parent. So,
    deleted Ads are still part of AdSet, Campaigns Insights. When user
    looks at Campaign metrics on FB they will see larger number than one
    we aggregate by rolling up insights from Ads visible to our system.

    The issue is not that we failed to pull reports right. It's because
    we have a snapshot of entities from a given past moment in time
    and actual list of entities is completely different if you roll
    the time back or forward.

If you find a logical issue with use of this table (say one that causes
some AdAccount to be permanently "not done") don't stress about it. Just
dump this table's data, which flushes the stale guesses out and allows
fresh guesses to be met with fresh reality a little better, until it, again,
drifts into staleness and need to be flushed.

It's also possible to have a record here where we clearly have record of getting
data for given Entity ID but we were not able to infer its existence.
(Say deleted AdSet's insights show up on per-AdAccount level=AS report, but
 there is no longer any record of that AdSet when you ask for entities
 per that AdAccount.)

Stale expectations that cannot be ever fulfilled (say due to
deleted Entities for which we did not collect some reports in-time
when they were not deletes) will permanently mark parent AdAccount as
"not done" forever.

I did not come up with a clean way to have a reliable "auto-clean stale guesses"
code *per sweep* as we are always working on partial fill of the data per sweep.
If you did not create an expectation for a given piece of data in this sweep,
is it fair to remove it? How do you know you are done with the sweep? (you don't)
The closest we can get to it is by looking at the expectation datetime and
have some job cleaning away un-covered expectations with (arbitrary) "very old"
datetimes of last expectation. Maybe add a worker for that clean up. Without that
"false negatives" ("inferred not done" that are actually false alarm) will be our
constant annoyance.

"""

from common.id_tools import parse_id, generate_id, JobIdParts, fields as job_id_fields
from common.memoize import MemoizeMixin, memoized_property
from config import dynamodb as dynamodb_config

from .base import BaseMeta, BaseModel, attributes


class JobReport(BaseModel, MemoizeMixin):
    # this table is split from JobReportEntityExpectation
    # in order to allow blind massive upserts of JobReportEntityExpectation
    # and JobReportEntityOutcome records without caring about
    # present values.
    # Reading these tables combined is very inefficient
    # (O(kn) where k is number of pages in JobReportEntityExpectation
    #  and n number of entities in the system)
    # compared to reading from what unified table could have been,
    # but we care much more about fast idempotent writes in batches
    # more than fast occasional reads.

    Meta = BaseMeta(
        dynamodb_config.JOB_REPORT_TABLE
    )

    # value of job_id here could be super weird.
    # It's actually the value of JobReportEntityExpectation.job_id
    # which there never has entity_id, entity_type filled in
    # but here that exact same ID is used as template and
    # entity_id, entity_type are filled in.
    # In this job_id you may see both, entity_type=Campaign and
    # report_variant=Campaign - not something you would see
    # in a wild. This is done just for one reason - create
    # an ID that is compound of JobReportEntityExpectation.job_id
    # and entity_id for use in this table only.
    job_id = attributes.UnicodeAttribute(hash_key=True, attr_name='jid')

    last_progress_dt = attributes.UTCDateTimeAttribute(null=True, attr_name='pdt')
    last_progress_stage_id = attributes.NumberAttribute(null=True, attr_name='pstid')
    last_progress_sweep_id = attributes.UnicodeAttribute(null=True, attr_name='psid')

    last_success_dt = attributes.UTCDateTimeAttribute(null=True, attr_name='sdt')
    last_success_sweep_id = attributes.UnicodeAttribute(null=True, attr_name='ssid')

    last_failure_dt = attributes.UTCDateTimeAttribute(null=True, attr_name='fdt')
    last_failure_stage_id = attributes.NumberAttribute(null=True, attr_name='fstid')
    last_failure_sweep_id = attributes.UnicodeAttribute(null=True, attr_name='fsid')

    last_failure_error = attributes.UnicodeAttribute(null=True, attr_name='fmessage')
    last_failure_bucket = attributes.NumberAttribute(null=True, attr_name='fb')

    last_total_running_time = attributes.NumberAttribute(null=True, attr_name='trt')
    last_total_datapoint_count = attributes.NumberAttribute(null=True, attr_name='tdc')

    last_partial_running_time = attributes.NumberAttribute(null=True, attr_name='prt')
    last_partial_datapoint_count = attributes.NumberAttribute(null=True, attr_name='pdc')


def sync_schema(brute_force=False):
    """
    In order to push fidelity and maintenance of table "migrations"
    closer to the code where the models are migrated, this is where
    we'll hook up generically-reused API for upserting our tables.
    Call this from some centralized `sync_schema` script
    """
    from pynamodb.exceptions import TableError, TableDoesNotExist

    tables = [
        JobReport
    ]

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

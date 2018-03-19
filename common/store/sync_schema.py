from . import (
    entities,
    entityreport,
    jobreport,
    scope,
    sweepentityreport,
)


modules_with_models = [
    entities,
    entityreport,
    jobreport,
    scope,
    sweepentityreport,
]


def sync_schema(brute_force=False):
    """
    In order to push fidelity and maintenance of table "migrations"
    closer to the code where the models are migrated, this is where
    we'll hook up generically-reused API for upserting our tables.
    Call this from some centralized `sync_schema` script
    """

    for module in modules_with_models:
        module.sync_schema(brute_force)

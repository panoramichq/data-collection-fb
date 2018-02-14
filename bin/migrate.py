# temporary place for DB schema application
# Dynamo does not really have "migrate" more like "set"
# Running this again and again should be fine (in testing with brute_force=True)
# Long term, this should do proper upsert of schema in prod too.
import sys
sys.path.insert(0, '.')

from common.store.sync_schema import sync_schema

if __name__ == '__main__':
    sync_schema(brute_force=True)

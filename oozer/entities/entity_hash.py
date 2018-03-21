import xxhash

from collections import namedtuple
from facebookads.adobjects import ad


from oozer.common.enum import FB_CAMPAIGN_MODEL, FB_ADSET_MODEL, FB_AD_MODEL
from oozer.common.facebook_api import get_default_fields


class EntityHash(namedtuple('EntityHash', ['data', 'fields'])):
    """
    Container for the hash to make it a little bit nicer
    """

    def __eq__(self, other):
        """
        Add equality operator for easy checking
        """
        return self.data == other.data and self.fields == other.fields


def checksum_entity(entity, fields=None):
    """
    Compute a hash of the entity fields that we consider stable, to be able
    to tell apart entities that have / have not changed in between runs.

    This method requires an intrinsic knowledge of "what the entity is".

    :return EntityHash: The hashes for the entity itself and
        and fields hashed
    """

    # Drop fields we don't care about
    blacklist = {
    }

    fields = fields or get_default_fields(entity.__class__)

    # Run through blacklist
    fields = filter(lambda f: f not in blacklist[entity.__class__], fields)

    raw_data = entity.export_all_data()

    data_hash = xxhash.xxh64()
    fields_hash = xxhash.xxh64()

    for field in fields:
        data_hash.update(str(raw_data.get(field, '')))
        fields_hash.update(field)

    return EntityHash(
        data=data_hash.hexdigest(),
        fields=fields_hash.hexdigest()
    )


def checksum_from_job_context(job_context, entity_id):
    """
    Recreate the EntityHash object from JobContext provided

    :param JobContext job_context: The provided job context
    :param string entity_id:

    :return EntityHash: The reconstructed EntityHash object
    """
    current_hash_raw = job_context.entity_checksums.get(
        entity_id, (None, None)
    )
    return EntityHash(*current_hash_raw)

import uuid


def get_string_id():
    # we may create some compound IDs by concatenating over minus
    # so, removing minus just in case.
    return str(uuid.uuid4()).replace('-', '')

_list_types = (list, tuple)


def collapse_fields_children(fields):
    """
    Helper function that collapses our internal expression of nested
    fields into an FB-specific (curlies and all) list of fields.

    This code is trying to address 2 problems:

    Note that we don't need to collapse the top-level list into single string.
    FB Ads SDK actually takes a list of fields as `fields` param.
    However, FB Ads SDK does very simple `params['fields'] = ','.join(fields)`
    with these. So, while keeping top-level fields a list is fine,
    nested fields must be collapsed into on string dangling under parent field string.

    Recursive nesting is supported

    (Because we use positional iterable for list of fields and positional iterable
     for definition of parent-children field pairs, using same type - list - may be
     a bit confusing for presentation. Allowing tuple type (along with list)
     as container for the pair.)

    Example::

        [
          'a',
          (
            'b',
            [
              'ba',
              'bb',
              (
                'bc',
                [
                  'bca',
                  'bcb'
                ]
              ),
              'bd'
            ]
          ),
          'c'
        ]

        # converts to

        [
          'a',
          'b{ba,bb,bc{bca,bcb},bd}',
          'c'
        ]

    :param fields:
    :type fields: list or tuple or set
    :rtype: str
    """

    return [
        field[0] + '{' + ','.join(collapse_fields_children(field[1])) + '}' if isinstance(field, _list_types) else field
        for field in fields
    ]

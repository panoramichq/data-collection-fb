import os
import sys


CONFIG_ENV_VAR_PREFIX = 'APP_'


def update_from_env(module_path, config_root_module='config', env_var_prefix=CONFIG_ENV_VAR_PREFIX):
    """
    This function, when ran against some module containing attributes that we treat as config attributes,
    allows us to override deep config module attributes with env vars.

    For example, given attribute `CONCURRENCY` stored in code in module `config.celery` as:

        config.celery.CONCURRENCY = 1

    and an environment variable `MYAPP_CELERY_CONCURRENCY` with value `5`, after running this function
    with arguments `update_from_env('config.celery', 'config', 'MYAPP_')` the module's attribute, at runtime,
    will be patched to have value equal to that of the environment variable.

        config.celery.CONCURRENCY = 5  # note that it's not '5'. Auto-retyping to int in effect

    This approach to applying env vars to config defaults allows you to avoid needing to have long,
    manually-managed mapping config files like:

        CELERY_BROKER_URL = environ.get('CELERY_BROKER_URL', 'redis://redis:6379')
        CELERY_RESULT_BACKEND_URL = environ.get('CELERY_RESULT_BACKEND_URL', 'redis://redis:6379')
        CELERY_TASK_RESULT_EXPIRES = environ.get('CELERY_TASK_RESULT_EXPIRES', timedelta(days=1))

    This approach also simplifies splits / namespacing of env vars and config module hives
    into thematic config trees (as opposed to one long flat file for all configs in the system)

    :param module_path:
    :param config_root_module:
    :param env_var_prefix:
    :return:
    """

    try:
        config_module = sys.modules[module_path]
    except KeyError:
        raise ValueError(f'Module {module_path} does not exist')

    config_root_module_path_prefix_parts = config_root_module.strip('.').split('.')
    affected_module_path_parts = module_path.split('.')

    # for given config submodule, say `config.celery`
    # config_module_prefix must be a subset of that path.
    # `config.` config_module_prefix value should work.
    # `api.config.` config_module_prefix value should NOT work.
    #  as that allows values in some other module (not that one calling
    #  this code) to be updated by accident
    if affected_module_path_parts[:len(config_root_module_path_prefix_parts)] != config_root_module_path_prefix_parts:
        raise ValueError(
            f'Module "{module_path}" is outside of config module "{config_root_module}" tree.'
        )

    env_var_prefix_with_full_module_path = '_'.join(
        [env_var_prefix.strip('_').upper()] +
        [
            part.upper()
            for part in affected_module_path_parts[len(config_root_module_path_prefix_parts):]
        ]
    ) + '_'

    env_var_prefix_len = len(env_var_prefix_with_full_module_path)
    env_vars_found = [
        (env_key[env_var_prefix_len:].upper(), os.environ[env_key])
        for env_key in os.environ.keys()
        if env_key.startswith(env_var_prefix_with_full_module_path)
    ]

    # Because all env var values are strings
    # existing value's type becomes the guide for how to convert the
    # incoming env var value. Say, if present config module attr is
    #  config.celery.concurrency = 3 # type: int
    # then we convert the values coming in from env vars into same type - int
    # However, some values don't convert directly from strings to proper values:
    falsy_bool_as_string = {'False', 'false', 'off', 'no', '0'}
    # Others there is no point reboxing (string into string) or there is no
    # good guide for conversion (when it's None to begin with)
    no_conversion = {str, type(None)}

    for key, value in env_vars_found:
        # Since we can't rely on case sensitivity in env vars, we try lower-case
        # and default to upper case if we don't find it
        key = key.lower() if getattr(config_module, key.lower(), None) else key

        ExistingValueType = type(getattr(config_module, key, None))

        if ExistingValueType is bool:
            value = False if value in falsy_bool_as_string else value

        setattr(
            config_module,
            key,
            value if ExistingValueType in no_conversion else ExistingValueType(value)
        )
        # note that ExistingValueType(value) does not work for complex value types like lists
        # no, if we run into that problem, will need to rethink this.
        # Until then, let's just be happy that we deal with auto-reboxing of int and bool

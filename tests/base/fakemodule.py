import sys
import types

from contextlib import contextmanager


@contextmanager
def FakeModule(module_name, module_body=None):
    """
    Context manager that creates a fake, shallow module for testing and removes it on exit

    Example:

        with FakeModule('a.b.c', 'x = 1') as m:
            assert m.x == 1
            assert 'a.b.c' in sys.modules
        assert 'a.b.c' not in sys.modules

    :param str module_name: dot-delimited path for the module to be created
    :param str module_body: Optional. If provided, full text of the body of the module
    :return: Module created within the context
    :rtype: types.ModuleType
    """

    _modules_to_remove = []
    _prior_parts = []
    module = None

    # need to insert preliminary steps in the path
    for name_part in module_name.split('.'):
        module_path = '.'.join(_prior_parts + [name_part])
        if module_path not in sys.modules:
            module = types.ModuleType(module_path)
            _modules_to_remove.append(module_path)
            sys.modules[module_path] = module
        _prior_parts.append(name_part)

    if not module:
        raise ValueError(f'Module name "{module_name}" cannot be faked as it already exists')

    if module_body:
        exec(module_body, module.__dict__)

    yield module

    for module_path in _modules_to_remove:
        del sys.modules[module_path]

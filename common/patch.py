import gevent
import gevent.monkey

_patched = False


def patch_event_loop():
    """
    Tries to monkeypatch with Gevent only once no matter how many times called

    Typically one can rely on fact that modules are imported once only and just
    have patching done in-line at module import. All subsequent imports of same
    module just return same exact instance, effectively achieving the same
    However, such import without use just looks odd and feels icky in the code
    Plus, flake8 complains etc etc... So, setting on very explicit approach
    like this one - import and call this function, which, in -turn relies on
    global singleton for avoiding to patch twice.
    """
    global _patched
    if not _patched:
        _patched = True
        gevent.monkey.patch_all()

"""
Centralized entry point mostly designed for hiding the intricacies of
setting up Celery worker and presenting it all as simple command
that can act as entry point for container.

Here we act on some environment variables for setting up correct
Celery queue names. These environment variables are not readily available
outside of the container, so it would be not cool to force external
scripts to imitate what we can do with ease inside of the container.

Run this to see the options / commands:

    python app.py --help

Example command:

    python app.py worker longrunning

"""
# this must be first import in our entry point
import common.patch
common.patch.patch_event_loop()

import argparse
import sys

from common.configure_logging import configure_logging
configure_logging()


from common.celeryapp import RoutingKey, get_celery_app, pad_with_build_id


class _CommandLineValues(argparse.Namespace):
    """
    This is just a mock representation of argparse.Namespace object
    extended with attribute we expect when it comes out of parser.

    This is done entirely just to make IDE think it understands the object
    and make it auto-complete attribute names.
    """
    command = 'str'
    worker_type = 'str'


def process_celery_worker_command(command_line_values):
    """
    :param _CommandLineValues command_line_values:
    """
    celery_app = get_celery_app()

    celery_worker_args = [
        'worker',
        '--pool=gevent',
        '--autoscale=1000,30',
        # Had an unfortunate occasion of bringing
        # a Redis instance to near-death with large
        # amount of network IO when these were on
        # and many workers are present.
        # Contemplate going "without" on these
        '--without-heartbeat',
        '--without-mingle',
        '--without-gossip',
        '--queues', pad_with_build_id(command_line_values.worker_type)
    ]
    celery_app.worker_main(celery_worker_args)


commands = {
    'worker': process_celery_worker_command
}


def parse_args(argv):
    """
    This is an overkill for one command, but this way
    we start on the right arg-parsing track for management
    of entry points.

    :param list argv:
    :rtype: _CommandLineValues
    """
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    subparsers = parser.add_subparsers(
        dest='command',
        title='Possible Commands'
    )

    worker_subparser = subparsers.add_parser('worker')
    worker_subparser.add_argument(
        'worker_type',
        choices=RoutingKey.ALL,
        help='Pick Celery routing key value this worker will be responsible for',
    )

    return parser.parse_args(argv)


if __name__ == '__main__':
    command_line_values = parse_args(sys.argv[1:])
    if command_line_values.command not in commands:
        print('Try adding "--help" to this command')  # todo: make it spiffier by printing pretty help.
        exit(1)
    commands[command_line_values.command](command_line_values)

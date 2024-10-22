# flake8: noqa: E722
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

from dotenv import load_dotenv

load_dotenv()

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
    port = '5555'


class StarterWorkerType:

    sweep = 'sweep'
    sweep_no_wait = 'sweep_no_wait'
    sweeps_loop = 'sweeps_loop'

    ALL = {sweep, sweep_no_wait, sweeps_loop}


def process_celery_worker_command(command_line_values):
    """
    :param _CommandLineValues command_line_values:
    """
    celery_app = get_celery_app()

    celery_worker_args = [
        'worker',
        '--pool=gevent',
        '--autoscale=100,30',
        # Had an unfortunate occasion of bringing
        # a Redis instance to near-death with large
        # amount of network IO when these were on
        # and many workers are present.
        # Contemplate going "without" on these
        # '--without-heartbeat',
        # '--without-mingle',
        # '--without-gossip',
        # However,
        # - Flower does not work without this
        # - While doing research found a number of bugs filed for obscure "result management"
        #   failures when these are disabled. Because these are not commonly used switches,
        #   majority of the world runs systems with them on, and, as result, majority of the
        #   result store kinks are caught with this stuff enabled. After trying to chase an
        #   explanation for why in random scenarios task.delay().join() never resolves (result
        #   is never returned), not leaving any weirdness on the table. Commenting these switches
        #   out is removing weirdness. (Yeah, I am going superstitious on you there :) )
        # Temporarily ignoring prescribed worker_type values
        # and assigning all possible routing keys to all workers.
        # Notice that we are purposefully still keep two separate queues,
        # even though they are processed by same workers.
        # one of these is effectively "high priority" line.
        # Even if it's same workers working on both,
        # items added to the "high priority" line get to
        # worker sooner because there are very few competitors in that line.
        # The other line may have thousands more tasks.
        '--queues',
        ','.join([pad_with_build_id(routing_key) for routing_key in RoutingKey.ALL])
        # '--queues', pad_with_build_id(command_line_values.worker_type)
    ]
    celery_app.worker_main(celery_worker_args)


def process_celery_flower_command(command_line_values):
    """
    :param _CommandLineValues command_line_values:
    """
    celery_app = get_celery_app()

    command_args = ['celery', 'flower', f'--port={command_line_values.port}']
    celery_app.start(command_args)


def process_start_command(command_line_values):
    """
    :param _CommandLineValues command_line_values:
    """

    if command_line_values.worker_type == StarterWorkerType.sweep_no_wait:
        from oozer.full_loop import run_sweep

        run_sweep()
        return

    # ** `sweep` is Default for Stage / Prod **
    # This mode of operation runs just one sweep, waits recommended time and quits.
    # This mode makes no sense until you realize how we provision / manage this
    # container in the sky.
    # We are using AWS Fargate and instruct the sweep controller container to be
    # restarted if it exits.
    # This mode of operation allows for several benefits:
    # - Compared to run_sweeps_forever loop, which never quits, here we
    #   benefit from all memory leakage to be flushed every sweep.
    # - Compared to sweep_no_wait, here we internalize the management of
    #   spacer time between sweep restarts (to avoid running it too frequently)
    # This mode of operation exists only because of AWS Fargate auto-keep-alive
    # setting we set.
    # In local development, run_sweeps_forever and run_sweep make more sense.
    if command_line_values.worker_type == StarterWorkerType.sweep:
        from oozer.full_loop import run_sweep_and_sleep

        run_sweep_and_sleep()
        return

    if command_line_values.worker_type == StarterWorkerType.sweeps_loop:
        from oozer.full_loop import run_sweeps_forever

        run_sweeps_forever()
        return

    # we never get values that are not on the list of valid ones
    # OptParser complains about that first, so, here we only get the ones
    # we declare in opt parser config as supported options.


commands = {
    'flower': process_celery_flower_command,
    'start': process_start_command,
    'worker': process_celery_worker_command,
}


def parse_args(argv):
    """
    This is an overkill for one command, but this way
    we start on the right arg-parsing track for management
    of entry points.

    :param list argv:
    :rtype: _CommandLineValues
    """
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)

    subparsers = parser.add_subparsers(
        # choices=commands.keys(),
        dest='command',
        title='Possible Commands',
    )

    worker_subparser = subparsers.add_parser('worker')
    worker_subparser.add_argument(
        'worker_type', choices=RoutingKey.ALL, help='Pick Celery routing key value this worker will be responsible for'
    )

    starter_subparser = subparsers.add_parser('start')
    starter_subparser.add_argument(
        'worker_type',
        choices=StarterWorkerType.ALL,
        help='Command that makes it ever slightly easier to start certain worker types.',
    )

    starter_subparser = subparsers.add_parser('flower')
    starter_subparser.add_argument('port', help='Port on which Celery Flower will serve the UI.')

    return parser.parse_args(argv)


if __name__ == '__main__':
    command_line_values = parse_args(sys.argv[1:])
    if command_line_values.command not in commands:
        print('Try adding "--help" to this command')  # todo: make it spiffier by printing pretty help.
        exit(1)
    commands[command_line_values.command](command_line_values)

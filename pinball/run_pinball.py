# Copyright 2015, Pinterest, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Command line tool to start token master and workflow workers."""
import argparse
import gc
import guppy
import signal
import socket
import sys
import time
import threading

from django.core import management
from pinball.config.pinball_config import PinballConfig
from pinball.config.utils import get_log
from pinball.config.utils import master_name
from pinball.master.factory import Factory
from pinball.ui import cache_thread


__author__ = 'Pawel Garbacki, Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = ['Pawel Garbacki', 'Mao Ye']
__license__ = 'Apache'
__version__ = '2.0'


LOG = get_log('pinball.run_pinball')

DbStore = None
Scheduler = None
Emailer = None
Worker = None


def _pinball_imports():
    """Import Pinball modules.

    The reason why these imports are not at the top level is that some of the
    imported code (db models initializing table names) depends on parameters
    passed on the command line (master name).  Those imports need to be delayed
    until after command line parameter parsing.
    """

    global DbStore
    from pinball.persistence.store import DbStore
    assert DbStore

    global Scheduler
    from pinball.scheduler.scheduler import Scheduler
    assert Scheduler

    global Emailer
    from pinball.workflow.emailer import Emailer
    assert Emailer

    global Worker
    from pinball.workflow.worker import Worker
    assert Worker


def _create_scheduler(factory, emailer):
    result = threading.Thread(target=_run_scheduler, args=[factory, emailer])
    result.daemon = True
    result.start()
    return result


def _run_scheduler(factory, emailer):
    client = factory.get_client()
    scheduler = Scheduler(client, DbStore(), emailer)
    scheduler.run()


def _create_workers(num_workers, factory, emailer):
    threads = []
    # A delay between starting individual workers.  We space starting new
    # workers to prevent overwhelming the master.  The interval should be long
    # enough to give a worker time to connect to the master but not so long
    # that it would substantially delay processing.
    sleep_interval = 5. / num_workers
    sleep_interval = max(sleep_interval, 1)
    for _ in range(0, num_workers):
        thread = threading.Thread(target=_run_worker, args=[factory, emailer])
        thread.daemon = True
        threads.append(thread)
        thread.start()
        time.sleep(sleep_interval)
    return threads


def _run_worker(factory, emailer):
    client = factory.get_client()
    worker = Worker(client, DbStore(), emailer)
    worker.run()


def _wait_for_threads(threads):
    finished_threads = 0
    total_threads = len(threads)
    LOG.info('Waiting for %d thread(s) to finish' % total_threads)
    # We cannot simply go over the list of threads and join them one by one
    # because join() does not propagate KeyboardInterrupt.
    while threads:
        new_threads = []
        found_finished = False
        for thread in threads:
            if thread.isAlive():
                new_threads.append(thread)
            else:
                found_finished = True
                thread.join()
                finished_threads += 1
                LOG.info('Thread %d/%d finished' % (finished_threads,
                                                    total_threads))
        threads = new_threads
        if not found_finished:
            time.sleep(5)
    LOG.info('Exiting')
    sys.exit()


def _siguser1_handler(sig, frame):
    """Signal handler showing some memory info.

    Args:
        sig: The captured signal.
        frame: The current stack frame.
    """
    gc.collect()
    heapy = guppy.hpy()
    heap = heapy.heap()
    for i in range(0, 5):
        LOG.info(heap)
        heap = heap.more
    relative_memory = heapy.heap().get_rp(50)
    for i in range(0, 5):
        LOG.info(relative_memory)
        relative_memory = relative_memory.more
    heapy.setref()


def _register_signal_listener():
    """Set up a signal handler showing memory stats."""
    signal.signal(signal.SIGUSR1, _siguser1_handler)


def main():
    _register_signal_listener()

    parser = argparse.ArgumentParser(
        description='Start Pinball master and workers.')
    parser.add_argument(
        '-c',
        '--config_file',
        dest='config_file',
        required=True,
        help='full path to the pinball setting configure file')
    parser.add_argument(
        '-m',
        '--mode',
        dest='mode',
        choices=['master', 'scheduler', 'workers', 'ui'],
        default='master',
        help='execution mode')

    options = parser.parse_args(sys.argv[1:])
    PinballConfig.parse(options.config_file)

    if hasattr(PinballConfig, 'MASTER_NAME') and PinballConfig.MASTER_NAME:
        master_name(PinballConfig.MASTER_NAME)
    _pinball_imports()
    if PinballConfig.UI_HOST:
        emailer = Emailer(PinballConfig.UI_HOST, 80)
    else:
        emailer = Emailer(socket.gethostname(), PinballConfig.UI_PORT)

    if options.mode == 'ui':
        hostport = '%s:%d' % (socket.gethostname(), PinballConfig.UI_PORT)
        cache_thread.start_cache_thread(DbStore())
        if not PinballConfig.UI_HOST:
            hostport = 'localhost:%d' % PinballConfig.UI_PORT

        # Disable reloader to prevent auto refresh on file changes.  The
        # problem with auto-refresh is that it starts multiple processes.  Some
        # of those processes will become orphans if we kill the UI in a wrong
        # way.
        management.call_command('runserver', hostport, interactive=False,
                                use_reloader=False)
        return

    factory = Factory(master_hostname=PinballConfig.MASTER_HOST,
                      master_port=PinballConfig.MASTER_PORT)
    threads = []
    if options.mode == 'master':
        factory.create_master(DbStore())
    elif options.mode == 'scheduler':
        threads.append(_create_scheduler(factory, emailer))
    else:
        assert options.mode == 'workers'
        if PinballConfig.UI_HOST:
            emailer = Emailer(PinballConfig.UI_HOST, 80)
        else:
            emailer = Emailer(socket.gethostname(), PinballConfig.UI_PORT)
        threads = _create_workers(PinballConfig.WORKERS, factory, emailer)

    try:
        if options.mode == 'master':
            factory.run_master_server()
        else:
            _wait_for_threads(threads)
    except KeyboardInterrupt:
        LOG.info('Exiting')
        sys.exit()


if __name__ == '__main__':
    main()

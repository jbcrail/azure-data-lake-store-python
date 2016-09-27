# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import os
import pytest
import time

from adlfs.core import AzureDLPath
from adlfs.transfer import ADLTransferClient
from tests.testing import azure, posix


@pytest.mark.skipif(True, reason="skip until resolve timing issue")
def test_interrupt(azure):
    def transfer(adlfs, src, dst, offset, size, blocksize, shutdown_event=None):
        while shutdown_event and not shutdown_event.is_set():
            time.sleep(0.1)
        return size, None

    client = ADLTransferClient(azure, 'foobar', transfer=transfer, chunksize=1,
                               tmp_path=None)
    client.submit('foo', 'bar', 16)
    client.run(monitor=False)
    time.sleep(1)
    client.shutdown()
    client.monitor()

    assert client.progress.successful


def test_submit_and_run(azure):
    def transfer(adlfs, src, dst, offset, size, blocksize, shutdown_event=None):
        time.sleep(0.1)
        return size, None

    client = ADLTransferClient(azure, 'foobar', transfer=transfer, chunksize=8,
                               tmp_path=None)

    client.submit('foo', 'bar', 16)
    client.submit('abc', '123', 8)

    assert len(client.progress.files) == 2
    assert len([f.chunks for f in client.progress.files])

    assert all([f.state == 'pending' for f in client.progress.files])
    assert all([chunk.state == 'pending' for f in client.progress.files
                                         for chunk in f.chunks])

    expected = {('bar', 0), ('bar', 8), ('123', 0)}
    assert {(chunk.name, chunk.offset) for f in client.progress.files
                                       for chunk in f.chunks} == expected

    client.run(monitor=False)
    client.monitor(timeout=2.0)

    assert client.progress.successful
    assert all([chunk.state == 'finished' for f in client.progress.files
                                          for chunk in f.chunks])
    assert all([chunk.expected == chunk.actual for f in client.progress.files
                                               for chunk in f.chunks])


def test_temporary_path(azure):
    def transfer(adlfs, src, dst, offset, size, blocksize, shutdown_event=None):
        time.sleep(0.1)
        return size, None

    client = ADLTransferClient(azure, 'foobar', transfer=transfer, chunksize=8,
                               tmp_unique=False)
    client.submit('foo', AzureDLPath('bar'), 16)

    assert os.path.dirname(posix(client.progress.files[0].chunks[0].name)) == '/tmp'

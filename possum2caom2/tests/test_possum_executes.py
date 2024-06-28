# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2024.                            (c) 2024.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 4 $
#
# ***********************************************************************
#

import logging

from datetime import datetime, timedelta, timezone
from glob import glob
from os import listdir
from os.path import exists
from shutil import copyfile

from caom2utils.data_util import get_local_file_headers
from possum2caom2 import possum_execute, storage_name
from caom2pipe.manage_composable import CadcException, ExecutionReporter, make_datetime, Observable, read_obs_from_file
from caom2pipe.manage_composable import State, TaskType
from mock import ANY, call, Mock, patch, PropertyMock


def test_renaming_options():
    tests = {
        'POSSUM.band1.0204-41.10187.i.fits': 'PSM_944MHz_20asec_0204-4100_10187_i_v1.fits',
        'POSSUM.mfs.band1.0144-46_0214-46_0204-41.10314.i.fits': 'PSM_944MHz_20asec_0144-4600_0214-4600_0204-4100_10314_i_v1.fits',
        'POSSUM.mfs.band1.0144+46_0214+46_0204-41.10314.i.fits': 'PSM_944MHz_20asec_0144+4600_0214+4600_0204-4100_10314_i_v1.fits',
        # TODO 'POSSUM.mfs.band1.1336-04A_1336-04B_1315-04B_1315-04A.6663.i.fits'
    }
    for original, renamed in tests.items():
        test_subject = storage_name.PossumName(original)
        assert test_subject.rename('') == renamed, f'wrong renamed {original}\n{test_subject.rename("")}\n{renamed}'


def test_execution_unit_start_stop(test_config, tmp_path):
    kwargs = {
        'clients': None,
        'data_source': None,
        'metadata_reader': None,
        'observable': None,
        'reporter': None,
        'prev_exec_dt': make_datetime('2023-10-28T20:47:49.000000000Z'),
        'exec_dt': make_datetime('2023-11-28T20:47:49.000000000Z'),
    }
    test_config.change_working_directory(tmp_path)
    test_config.cleanup_files_when_storing = True
    test_subject = possum_execute.ExecutionUnit(test_config, **kwargs)

    # preconditions
    test_files = listdir(tmp_path)
    assert len(test_files) == 0, 'directory should be empty'

    test_subject.start()

    test_files = listdir(tmp_path)
    assert len(test_files) == 1, 'directory should have a workspace directory'
    assert '2023-10-28T20_47_49_2023-11-28T20_47_49' in test_files, 'wrong working directory'

    test_subject.stop()

    test_files = listdir(tmp_path)
    assert len(test_files) == 0, 'post-condition directory should be cleaned up and empty'

    test_config.cleanup_files_when_storing = False
    test_subject = possum_execute.ExecutionUnit(test_config, **kwargs)

    # preconditions
    test_files = listdir(tmp_path)
    assert len(test_files) == 0, 'directory should be empty'

    test_subject.start()

    test_files = listdir(tmp_path)
    assert len(test_files) == 1, 'directory should have a workspace directory'
    assert '2023-10-28T20_47_49_2023-11-28T20_47_49' in test_files, 'wrong working directory'

    test_subject.stop()

    test_files = listdir(tmp_path)
    assert len(test_files) == 0, 'post-condition empty directory should not exist'


# need test_config parameter so StorageName.collection is set
@patch('possum2caom2.possum_execute.compute_md5sum')
@patch('caom2utils.data_util.get_local_headers_from_fits')
def test_remote_metadata_reader_file_info_and_todo_reader(header_mock, md5_mock, test_config, test_data_dir):
    header_mock.return_value = []
    md5_mock.return_value = 'abc'

    input_file = f'{test_data_dir}/storage_mock/rclone_lsjson.json'
    test_file_uri = 'cadc:POSSUM/PSM.band1.0049-51.10887.i.fits'
    test_subject = possum_execute.RemoteMetadataReader()

    with open(input_file) as f:
        test_subject.set_file_info(f.read())

    assert len(test_subject.file_info) == 4, 'wrong number of results'
    test_result = test_subject.file_info.get(test_file_uri)
    assert test_result is not None, 'expect a result'
    assert test_result.size == 4831848000, 'wrong size'
    assert test_result.file_type == 'application/fits', 'wrong file type'
    assert test_result.lastmod == datetime(2023, 11, 18, 20, 47, 50), 'wrong modification time'

    test_storage_name = test_subject.storage_names.get(test_file_uri)
    assert test_storage_name.file_uri == test_file_uri, 'wrong file uri'

    test_storage_name.rename('944MHz')
    final_file_name = 'PSM_944MHz_20asec_0049-5100_10887_i_v1.fits'
    assert test_storage_name.stage_names[0] == final_file_name
    test_storage_name_renamed = storage_name.PossumName(f'/tmp/{final_file_name}')
    test_subject_2 = possum_execute.TodoMetadataReader(test_subject)
    test_subject_2.set(test_storage_name_renamed)
    assert len(test_subject_2.file_info) == 1, 'wrong bit of file_info'
    test_file_info_result = test_subject_2.file_info.get(f'cadc:POSSUM/{final_file_name}')
    assert test_file_info_result.size == 4831848000, 'renamed wrong size'
    assert test_file_info_result.file_type == 'application/fits', 'renamed wrong file type'
    assert test_file_info_result.lastmod == datetime(2023, 11, 18, 20, 47, 50), 'renamed wrong modification time'
    assert test_file_info_result.md5sum == 'md5:abc', 'renamed wrong md5sum'
    assert len(test_subject_2.headers) == 1, 'wrong header content'


@patch('possum2caom2.possum_execute.exec_cmd')
@patch('possum2caom2.possum_execute.exec_cmd_info')
def test_remote_data_source(exec_cmd_info_mock, exec_cmd_mock, test_data_dir, test_config, tmp_path):
    with open(f'{test_data_dir}/storage_mock/rclone_lsjson.json') as f:
        exec_cmd_info_mock.return_value = f.read()

    exec_cmd_mock.side_effect = Mock()

    test_config.change_working_directory(tmp_path)
    test_start_key = 'test/acacia_possum/pawsey0980'
    test_start_time = make_datetime('2023-10-28T20:47:49.000000000Z')
    test_end_time = make_datetime('2023-11-28T20:47:49.000000000Z')
    State.write_bookmark(test_config.state_fqn, test_start_key, test_start_time)
    test_metadata_reader = possum_execute.RemoteMetadataReader()
    mock_1 = Mock()
    mock_2 = Mock()
    mock_3 = Mock()
    kwargs = {
        'clients': mock_1,
        'observable': mock_2,
        'reporter': mock_3,
    }
    test_subject = possum_execute.RemoteIncrementalDataSource(
        test_config,
        test_start_key,
        test_metadata_reader,
        **kwargs,
    )
    test_subject.reporter = mock_3
    test_subject.initialize_start_dt()
    assert test_subject.start_dt == datetime(2023, 10, 28, 20, 47, 49), 'start_dt'
    test_subject.initialize_end_dt()
    assert test_subject.end_dt == datetime(2023, 11, 18, 20, 47, 50), 'end_dt'
    test_result = test_subject.get_time_box_work(test_start_time, test_end_time)
    assert test_result is not None, 'expect a result'
    assert test_result._clients == mock_1, 'clients'
    assert test_result._observable == mock_2, 'observable'
    assert test_result._reporter == mock_3, 'reporter'
    assert test_result._remote_metadata_reader == test_metadata_reader, 'reader'
    assert mock_3.capture_todo.called, 'capture_todo'


def test_state_runner_reporter(test_config, tmp_path, change_test_dir):
    # make sure the StateRunner goes through at least one time box check, and creates the
    # right log locations
    test_config.change_working_directory(tmp_path)
    test_config.task_types = [TaskType.STORE, TaskType.INGEST, TaskType.MODIFY]
    test_config.data_sources = ['test/acacia:possum1234']
    test_config.interval = 60 * 48  # work in time-boxes of 2 days => 60m * 48h
    test_organizer = Mock()
    # the time-box times, or, this is "when" the code looks
    test_start_time = make_datetime('2023-10-28T20:47:49.000000000Z')
    test_end_time = make_datetime('2023-11-28T20:47:49.000000000Z')
    test_data_source = Mock()
    end_time_mock = PropertyMock(return_value=test_end_time)
    start_time_mock = PropertyMock(return_value=test_start_time)
    type(test_data_source).end_dt = end_time_mock
    type(test_data_source).start_dt = start_time_mock
    # the execution times, or, this is "what" the code finds
    test_entry_time = make_datetime('2023-11-28T08:47:49.000000000Z')
    execution_unit_mock = Mock()
    type(execution_unit_mock).entry_dt = test_entry_time
    type(execution_unit_mock).num_entries = 1
    execution_unit_mock.do.return_value = 0
    test_data_source.get_time_box_work.return_value = execution_unit_mock
    test_data_sources = [test_data_source]
    test_observable = Mock()
    test_reporter = ExecutionReporter(test_config, test_observable)
    test_subject = possum_execute.ExecutionUnitStateRunner(
        test_config,
        test_organizer,
        test_data_sources,
        test_observable,
        test_reporter,
    )
    test_result = test_subject.run()
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'happy path'
    assert test_organizer.mock_calls == [], 'organizer'
    assert test_data_source.initialize_start_dt.called, 'initialize_start_dt'
    assert test_data_source.initialize_start_dt.call_count == 1, 'initialize_start_dt count'
    assert test_data_source.initialize_end_dt.called, 'initialize_end_dt'
    assert test_data_source.initialize_end_dt.call_count == 1, 'initialize_end_dt count'
    assert test_data_source.get_time_box_work.called, 'get_time_box_work'
    # 16 == number of days / 2 between the start and end times
    assert test_data_source.get_time_box_work.call_count == 16, 'get_time_box_work count'
    test_observable.assert_has_calls([]), 'observable calls'
    assert exists(test_config.failure_fqn), 'failure'
    assert exists(test_config.progress_fqn), 'progress'
    assert exists(test_config.success_fqn), 'success'
    assert exists(test_config.retry_fqn), 'retries'
    assert exists(test_config.total_retry_fqn), 'total_retries'
    assert exists(test_config.report_fqn), 'report'
    assert test_reporter.success == 0, 'reporter success'
    assert test_reporter.all == 0, 'reporter all'


@patch('possum2caom2.preview_augmentation.visit')
@patch('caom2utils.data_util.get_local_headers_from_fits')
@patch('possum2caom2.possum_execute.exec_cmd')
@patch('possum2caom2.possum_execute.exec_cmd_info')
def test_state_runner_nominal_multiple_files(
    exec_cmd_info_mock,
    exec_cmd_mock,
    header_mock,
    preview_mock,
    test_config,
    test_data_dir,
    tmp_path,
    change_test_dir,
):
    import logging
    # logging.getLogger().setLevel(logging.INFO)
    # test that three file get processed properly, and get left behind
    with open(f'{test_data_dir}/storage_mock/rclone_lsjson.json') as f:
        exec_cmd_info_mock.return_value = f.read()

    i_test_file = 'PSM.band1.0049-51.11092.i.fits'
    q_test_file = 'PSM.band2.1136-64.11836.q.fits'
    u_test_file = 'PSM.band2.1136-64.11485.u.fits'
    time_box_dir_name = '2023-10-28T20_47_49_2023-10-30T20_47_49'
    time_box_dir_name_2 = '2023-11-17T20_47_49_2023-11-18T20_47_50'
    def _exec_cmd_mock(arg1):
        logging.error(arg1)
        if arg1 == (
            f'rclone copy pawsey_test:acacia/possum1234 {tmp_path}/{time_box_dir_name} --max-age=2023-10-28T20:47:49 --min-age=2023-10-30T20:47:49 --include=*[iqu].fits'
        ):
            copyfile(
                f'{test_data_dir}/casda/PSM_pilot1_1367MHz_18asec_2013-5553_11261_t0_i_v1.fits.header',
                f'{tmp_path}/{time_box_dir_name}/{i_test_file}',
            )
            copyfile(
                f'{test_data_dir}/casda/PSM_pilot1_1368MHz_18asec_2031-5249_11073_i_v1.fits.header',
                f'{tmp_path}/{time_box_dir_name}/{q_test_file}',
            )
        elif arg1 == (
            f'rclone copy pawsey_test:acacia/possum1234 {tmp_path}/{time_box_dir_name_2} --max-age=2023-11-17T20:47:49 --min-age=2023-11-18T20:47:50 --include=*[iqu].fits'
        ):
            copyfile(
                f'{test_data_dir}/casda/PSM_pilot1_1368MHz_18asec_2031-5249_11073_q_v1.fits.header',
                f'{tmp_path}/{time_box_dir_name_2}/{u_test_file}',
            )
    exec_cmd_mock.side_effect = _exec_cmd_mock
    header_mock.side_effect = get_local_file_headers
    preview_mock.side_effect = (
        lambda x, working_directory, storage_name, log_file_directory, clients, observable, metadata_reader, config: x
    )
    test_config.change_working_directory(tmp_path)
    test_config.cleanup_files_when_storing = False
    test_config.task_types = [TaskType.STORE, TaskType.INGEST, TaskType.MODIFY]
    test_config.data_sources = ['pawsey_test/acacia/possum1234']
    test_config.data_source_extensions = ['.fits', '.fits.header']
    test_config.logging_level = 'INFO'
    test_config.interval = 60 * 48  # work in time-boxes of 2 days => 60m * 48h
    test_config.observe_execution = True
    test_organizer = Mock()
    # the time-box times, or, this is "when" the code looks
    test_start_time = make_datetime('2023-10-28T20:47:49.000000000Z')
    test_end_time = make_datetime('2023-11-18T20:47:50.000000000Z')
    State.write_bookmark(test_config.state_fqn, test_config.data_sources[0], test_start_time)
    test_metadata_reader = possum_execute.RemoteMetadataReader()
    test_observable = Observable(test_config)
    test_reporter = ExecutionReporter(test_config, test_observable)
    test_clients = Mock()
    test_clients.metadata_client.read.return_value = None
    test_observation = read_obs_from_file(f'{test_data_dir}/storage_mock/renaming_observation.xml')
    test_clients.server_side_ctor_client.read.side_effect = [test_observation, test_observation, test_observation]
    kwargs = {
        'clients': test_clients,
        'observable': test_observable,
        'reporter': test_reporter
    }
    test_data_source = possum_execute.RemoteIncrementalDataSource(
        test_config,
        test_config.data_sources[0],
        test_metadata_reader,
        **kwargs,
    )
    test_data_source.reporter = test_reporter
    test_data_sources = [test_data_source]
    test_subject = possum_execute.ExecutionUnitStateRunner(
        test_config,
        test_organizer,
        test_data_sources,
        test_observable,
        test_reporter,
    )
    test_result = test_subject.run()
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'happy path'
    assert test_organizer.mock_calls == [], 'organizer'
    assert test_clients.mock_calls == [
        call.data_client.put(f'{tmp_path}/{time_box_dir_name}', f'cadc:POSSUM/PSM_1368MHz_18asec_2031-5249_11073_i_v1.fits'),
        call.metadata_client.read('POSSUM', '1368MHz_18asec_2031-5249_11073_v1'),
        call.server_side_ctor_client.delete('POSSUM', '1368MHz_18asec_2031-5249_11073_v1'),
        call.server_side_ctor_client.create(ANY),
        call.server_side_ctor_client.read('POSSUM', '1368MHz_18asec_2031-5249_11073_v1'),
        call.metadata_client.create(ANY),
        call.data_client.put(f'{tmp_path}/{time_box_dir_name}', f'cadc:POSSUM/PSM_1367MHz_18asec_2013-5553_11261_i_v1.fits'),
        call.metadata_client.read('POSSUM', '1367MHz_18asec_2013-5553_11261_v1'),
        call.server_side_ctor_client.delete('POSSUM', '1367MHz_18asec_2013-5553_11261_v1'),
        call.server_side_ctor_client.create(ANY),
        call.server_side_ctor_client.read('POSSUM', '1367MHz_18asec_2013-5553_11261_v1'),
        call.metadata_client.create(ANY),
        call.data_client.put(f'{tmp_path}/{time_box_dir_name_2}', f'cadc:POSSUM/PSM_1368MHz_18asec_2031-5249_11073_q_v1.fits'),
        call.metadata_client.read('POSSUM', '1368MHz_18asec_2031-5249_11073_v1'),
        call.server_side_ctor_client.delete('POSSUM', '1368MHz_18asec_2031-5249_11073_v1'),
        call.server_side_ctor_client.create(ANY),
        call.server_side_ctor_client.read('POSSUM', '1368MHz_18asec_2031-5249_11073_v1'),
        call.metadata_client.create(ANY),
    ], f'clients {test_clients.mock_calls}'
    assert exists(test_config.rejected_fqn), f'rejected {test_config.rejected_fqn}'
    assert exists(test_config.observable_directory), f'metrics {test_config.observable_directory}'
    assert test_data_source.end_dt == test_end_time, 'end_dt'
    assert test_reporter.all == 4, f'wrong file count all {test_reporter.all}'
    assert test_reporter.success == 3, f'wrong file count success {test_reporter.success}'
    left_behind = listdir(f'{tmp_path}/{time_box_dir_name}')
    left_behind_2 = listdir(f'{tmp_path}/{time_box_dir_name_2}')
    assert len(left_behind) + len(left_behind_2) == 3, 'no files cleaned up'


@patch('possum2caom2.preview_augmentation.visit')
@patch('caom2utils.data_util.get_local_headers_from_fits')
@patch('possum2caom2.possum_execute.exec_cmd')
@patch('possum2caom2.possum_execute.exec_cmd_info')
def test_state_runner_clean_up_when_storing_with_retry(
    exec_cmd_info_mock, exec_cmd_mock, header_mock, visit_mock, test_config, test_data_dir, tmp_path, change_test_dir
):
    # one file gets cleaned up
    with open(f'{test_data_dir}/storage_mock/rclone_lsjson.json') as f:
        exec_cmd_info_mock.return_value = f.read()

    u_test_file = 'PSM.band2.1136-64.11485.u.fits'
    time_box_dir_name = '2023-10-28T20_47_49_2023-10-30T20_47_49'
    def _exec_cmd_mock(arg1):
        logging.error(arg1)
        if arg1 == (
            f'rclone copy pawsey_test:acacia/possum1234 {tmp_path}/{time_box_dir_name} --max-age=2023-10-28T20:47:49 --min-age=2023-10-30T20:47:49 --include=*[iqu].fits'
        ):
            copyfile(
                f'{test_data_dir}/casda/PSM_pilot1_1368MHz_18asec_2031-5249_11073_q_v1.fits.header',
                f'{tmp_path}/{time_box_dir_name}/{u_test_file}',
            )
    exec_cmd_mock.side_effect = _exec_cmd_mock
    header_mock.side_effect = get_local_file_headers
    visit_mock.side_effect = (
        lambda x, working_directory, storage_name, log_file_directory, clients, observable, metadata_reader, config: x
    )
    test_config.change_working_directory(tmp_path)
    test_config.cleanup_files_when_storing = True
    test_config.task_types = [TaskType.STORE, TaskType.INGEST, TaskType.MODIFY]
    test_config.data_sources = ['pawsey_test/acacia/possum1234']
    test_config.data_source_extensions = ['.fits', '.fits.header']
    test_config.logging_level = 'INFO'
    test_config.interval = 60 * 48  # work in time-boxes of 2 days => 60m * 48h
    test_config.observe_execution = True
    test_config.retry_failures = True
    test_config.retry_count = 1
    test_config.retry_decay = 0
    test_organizer = Mock()
    # the time-box times, or, this is "when" the code looks
    test_start_time = make_datetime('2023-10-28T20:47:49.000000000Z')
    test_end_time = make_datetime('2023-11-18T20:47:50.000000000Z')
    State.write_bookmark(test_config.state_fqn, test_config.data_sources[0], test_start_time)
    test_metadata_reader = possum_execute.RemoteMetadataReader()
    test_observable = Observable(test_config)
    test_reporter = ExecutionReporter(test_config, test_observable)
    test_clients = Mock()
    test_clients.metadata_client.read.side_effect = [CadcException, None]
    test_observation = read_obs_from_file(f'{test_data_dir}/storage_mock/renaming_observation.xml')
    test_clients.server_side_ctor_client.read.side_effect = [test_observation]
    kwargs = {
        'clients': test_clients,
        'observable': test_observable,
        'reporter': test_reporter
    }
    test_data_source = possum_execute.RemoteIncrementalDataSource(
        test_config,
        test_config.data_sources[0],
        test_metadata_reader,
        **kwargs,
    )
    test_data_source.reporter = test_reporter
    test_data_sources = [test_data_source]
    test_subject = possum_execute.ExecutionUnitStateRunner(
        test_config,
        test_organizer,
        test_data_sources,
        test_observable,
        test_reporter,
    )
    test_result = test_subject.run()
    assert test_result is not None, 'expect a result'
    assert test_result == -1, 'happy path with a retry'
    assert test_organizer.mock_calls == [], 'organizer'
    assert test_clients.mock_calls == [
        call.data_client.put(f'{tmp_path}/{time_box_dir_name}', f'cadc:POSSUM/PSM_1368MHz_18asec_2031-5249_11073_q_v1.fits'),
        call.metadata_client.read('POSSUM', '1368MHz_18asec_2031-5249_11073_v1'),
        # because there's a retry
        call.data_client.put(f'{tmp_path}/{time_box_dir_name}', f'cadc:POSSUM/PSM_1368MHz_18asec_2031-5249_11073_q_v1.fits'),
        call.metadata_client.read('POSSUM', '1368MHz_18asec_2031-5249_11073_v1'),
        call.server_side_ctor_client.delete('POSSUM', '1368MHz_18asec_2031-5249_11073_v1'),
        call.server_side_ctor_client.create(ANY),
        call.server_side_ctor_client.read('POSSUM', '1368MHz_18asec_2031-5249_11073_v1'),
        call.metadata_client.create(ANY),
    ], f'clients {test_clients.mock_calls}'
    assert exists(test_config.rejected_fqn), f'rejected {test_config.rejected_fqn}'
    assert exists(test_config.observable_directory), f'metrics {test_config.observable_directory}'
    assert test_data_source.end_dt == test_end_time, 'end_dt'
    assert test_reporter.all == 4, f'wrong file count all {test_reporter.all}'
    assert test_reporter.success == 1, f'wrong file count {test_reporter.success}'
    left_behind = glob(f'{tmp_path}/2*')
    assert len(left_behind) == 0, 'files should be cleaned up'


@patch('possum2caom2.possum_execute.exec_cmd')
@patch('possum2caom2.possum_execute.exec_cmd_info')
@patch('possum2caom2.possum_execute.RCloneClients')
def test_remote_execution(
    clients_mock, exec_cmd_info_mock, exec_cmd_mock, test_config, tmp_path, change_test_dir
):
    # execution path for "rclone lsjson" working, but not "rclone copy"
    # config
    test_config.change_working_directory(tmp_path)
    test_config.cleanup_files_when_storing = False
    test_config.task_types = [TaskType.STORE, TaskType.INGEST, TaskType.MODIFY]
    test_config.data_sources = ['pawsey_test/acacia/possum1234']
    test_config.data_source_extensions = ['.fits', '.fits.header']
    test_config.logging_level = 'INFO'
    test_config.interval = 60 * 48  # work in time-boxes of 2 days => 60m * 48h
    test_config.observe_execution = True
    test_config.write_to_file(test_config)

    # state
    tomorrow = datetime.now() + timedelta(days=1)
    State.write_bookmark(test_config.state_fqn, test_config.data_sources[0], tomorrow)

    # exec
    file_mod_time = tomorrow + timedelta(minutes=1)
    def _exec_cmd_info_mock(arg1):
        assert arg1.startswith(
            f'rclone lsjson pawsey_test:acacia/possum1234 --recursive --max-age={tomorrow.isoformat()} --include=*[iqu].fits'
        ), f'exec_cmd_info {arg1}'
        return (
            f'[{{\"Path\":\"components/0049-51/survey/i/PSM.0049-51.10887.i.fits\",\"Name\":'
            f'\"PSM.0049-51.10887.i.fits\",\"Size\":16787520,\"MimeType\":\"image/fits\",\"ModTime\":'
            f'\"{file_mod_time}\",\"IsDir\":false,\"Tier\":\"STANDARD\"}}]'
        )
    exec_cmd_info_mock.side_effect = _exec_cmd_info_mock
    test_working_directory = (
        f'{tmp_path}/{tomorrow.isoformat().replace(":", "_").replace(".", "_")}_'
        f'{file_mod_time.isoformat().replace(":", "_").replace(".", "_")}'
    )
    def _exec_cmd_mock(arg1):
        assert arg1.startswith(
            f'rclone copy pawsey_test:acacia/possum1234 {test_working_directory} --max-age={tomorrow.isoformat()} --min-age={file_mod_time.isoformat()} --include='
        ), f'exec_cmd {arg1}'
    exec_cmd_mock.side_effect = _exec_cmd_mock

    test_result = possum_execute.remote_execution()
    assert test_result == -1, 'expect failure result'
    assert clients_mock.data_client.mock_calls == [], f'client mock {clients_mock.data_client.mock_calls}'
    assert clients_mock.metadata_client.mock_calls == [], f'client mock {clients_mock.metadata_client.mock_calls}'
    assert (
        clients_mock.server_side_ctor_client.mock_calls == []
    ), f'client mock {clients_mock.server_side_ctor_client.mock_calls}'


@patch('possum2caom2.fits2caom2_augmentation.visit')
@patch('possum2caom2.possum_execute.RCloneClients')
def test_remote_execute_with_local_commands(
    clients_mock, visit_mock, test_config, test_data_dir, tmp_path, change_test_dir
):
    # execution path for local rclone
    test_config.change_working_directory(tmp_path)
    test_config.cleanup_files_when_storing = False
    test_config.task_types = [TaskType.STORE, TaskType.INGEST, TaskType.MODIFY]
    test_config.data_sources = [f'{test_data_dir}/rclone_test']
    test_config.data_source_extensions = ['.fits']
    test_config.logging_level = 'ERROR'
    test_config.interval = 60 * 48  # work in time-boxes of 2 days => 60m * 48h
    test_config.observe_execution = True
    test_config.write_to_file(test_config)

    # state
    start_dt = datetime(2024, 4, 16, 16, 33, 0)
    State.write_bookmark(test_config.state_fqn, test_config.data_sources[0], start_dt)
    # TODO - add an end time to this test so it doesn't take increasingly long amounts of time
    # 2024-04-16T18:40:58.751296025Z

    # mock returns
    # test_observation is purely for return values - it has nothing to do with the files from the test directory
    test_observation = read_obs_from_file(f'{test_data_dir}/storage_mock/renaming_observation.xml')
    clients_mock.return_value.server_side_ctor_client.read.return_value = None
    visit_mock.return_value = test_observation

    test_result = possum_execute.remote_execution()
    assert test_result == 0, 'expect success result'
    assert clients_mock.return_value.data_client.put.call_count == 5, f'client mock {clients_mock.return_value.data_client.put.call_count}'
    assert clients_mock.return_value.metadata_client.read.call_count == 5, f'metadata client call count'
    assert clients_mock.return_value.metadata_client.update.call_count == 5, f'metadata client call count'
    time_box_3 = '2024-04-20T16_33_00_2024-04-22T16_33_00'
    time_box_4 = '2024-04-28T16_33_00_2024-04-29T20_42_46'
    assert (
        clients_mock.return_value.data_client.put.mock_calls == [
            call(f'{tmp_path}/{time_box_3}', 'cadc:POSSUM/POSSUM.mfs.band1.2108+00A_2108+04B_2108+00B_2108+04A.5808.i.fits'),
            call(f'{tmp_path}/{time_box_4}', 'cadc:POSSUM/POSSUM.band1.0204-41.10187.i.fits'),
            call(f'{tmp_path}/{time_box_4}', 'cadc:POSSUM/POSSUM.band1.0204-41.10187.u.fits'),
            call(f'{tmp_path}/{time_box_4}', 'cadc:POSSUM/POSSUM.band2.1506-32A_1506-32B.9488.i.fits'),
            call(f'{tmp_path}/{time_box_4}', 'cadc:POSSUM/POSSUM.band1.0204-41.10187.q.fits'),
        ]
    ), clients_mock.return_value.data_client.put.mock_calls
    assert (
        clients_mock.return_value.server_side_ctor_client.mock_calls == []
    ), f'client mock {clients_mock.server_side_ctor_client.mock_calls}'


@patch('possum2caom2.possum_execute.ExecutionUnit.start')
def test_empty_listing(start_mock, test_config, test_data_dir, tmp_path, change_test_dir):
    test_config.change_working_directory(tmp_path)
    test_config.cleanup_files_when_storing = True
    test_config.task_types = [TaskType.STORE, TaskType.INGEST, TaskType.MODIFY]
    test_config.data_sources = [f'{test_data_dir}/storage_mock']
    test_config.data_source_extensions = ['.fits', '.fits.header']
    test_config.logging_level = 'DEBUG'
    test_config.interval = 60 * 48  # work in time-boxes of 2 days => 60m * 48h
    test_config.observe_execution = True
    test_config.retry_failures = False
    test_organizer = Mock()
    test_observable = Observable(test_config)
    test_reporter = ExecutionReporter(test_config, test_observable)
    # the time-box times, or, this is "when" the code looks
    test_start_time = make_datetime('2023-10-28T20:47:49.000000000Z')
    test_end_time = make_datetime('2023-11-18T20:47:50.000000000Z')
    State.write_bookmark(test_config.state_fqn, test_config.data_sources[0], test_start_time)
    test_metadata_reader = possum_execute.RemoteMetadataReader()
    test_observable = Observable(test_config)
    test_reporter = ExecutionReporter(test_config, test_observable)
    test_clients = Mock()
    kwargs = {
        'clients': test_clients,
        'observable': test_observable,
        'reporter': test_reporter
    }
    test_data_source = possum_execute.RemoteIncrementalDataSource(
        test_config,
        test_config.data_sources[0],
        test_metadata_reader,
        **kwargs,
    )
    end_time_mock = PropertyMock(return_value=test_end_time)
    type(test_data_source).end_dt = end_time_mock
    test_data_source.reporter = test_reporter
    test_data_sources = [test_data_source]
    test_subject = possum_execute.ExecutionUnitStateRunner(
        test_config,
        test_organizer,
        test_data_sources,
        test_observable,
        test_reporter,
    )
    test_result = test_subject.run()
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'happy path with no results listed'
    assert test_clients.data_client.put.call_count == 0, f'client mock {test_clients.data_client.put.call_count}'
    assert test_clients.metadata_client.read.call_count == 0, f'metadata client call count'
    assert test_clients.metadata_client.update.call_count == 0, f'metadata client call count'
    assert start_mock.call_count == 0, f'wrong start call count {start_mock.call_count}'

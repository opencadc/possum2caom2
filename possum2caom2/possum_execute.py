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

import glob
import json
import logging
import os
import shutil
import traceback

import numpy as np
from astropy import units
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.wcs import WCS
from astropy_healpix import HEALPix
from copy import deepcopy
from datetime import datetime

from cadcdata.storageinv import FileInfo
from caom2pipe.client_composable import ClientCollection, repo_create, repo_get, repo_update
from caom2pipe.data_source_composable import IncrementalDataSource, ListDirSeparateDataSource
from caom2pipe.execute_composable import OrganizeExecutes
from caom2pipe.manage_composable import CadcException, Config, compute_md5sum, create_dir, exec_cmd, exec_cmd_info
from caom2pipe.manage_composable import ExecutionReporter, increment_time, make_datetime, Observable, StorageName, TaskType
from caom2pipe.name_builder_composable import EntryBuilder
from caom2pipe.reader_composable import FileMetadataReader
from caom2pipe.run_composable import set_logging, StateRunner, TodoRunner
from caom2pipe.transfer_composable import CadcTransfer, Transfer
from caom2repo import CAOM2RepoClient
from caom2utils.data_util import get_file_type
from possum2caom2 import fits2caom2_augmentation, preview_augmentation, spectral_augmentation
from possum2caom2.storage_name import PossumName


__all__ = ['DATA_VISITORS', 'META_VISITORS', 'remote_execution']
META_VISITORS = [fits2caom2_augmentation]
DATA_VISITORS = [preview_augmentation, spectral_augmentation]


class RCloneClients(ClientCollection):

    def __init__(self, config):
        super().__init__(config)
        # TODO rclone credentials
        self._rclone_client = None
        if TaskType.SCRAPE in config.task_types:
            self._logger.info(f'SCRAPE\'ing data - no clients will be initialized.')
        else:
            self._server_side_ctor_client = CAOM2RepoClient(
                self._subject, config.logging_level, config.server_side_resource_id
            )

    @property
    def rclone_client(self):
        return self._rclone_client

    @property
    def server_side_ctor_client(self):
        return self._server_side_ctor_client


class RemoteMetadataReader(FileMetadataReader):
    def __init__(self):
        super().__init__()
        self._storage_names = {}
        self._max_dt = None

    @property
    def max_dt(self):
        return self._max_dt

    @property
    def storage_names(self):
        return self._storage_names

    def _retrieve_file_info(self, key, source_name):
        raise NotImplementedError

    def get_time_box_work_parameters(self, prev_exec_time, exec_time):
        self._logger.debug(f'Begin get_time_box_work_parameters from {prev_exec_time} to {exec_time}')
        count = 0
        max_time_box = prev_exec_time
        for entry in self._file_info.values():
            if prev_exec_time < entry.lastmod <= exec_time:
                count += 1
                max_time_box = max(prev_exec_time, entry.lastmod)
        self._logger.debug(f'End get_time_box_work_parameters with count {count}')
        return count, max_time_box

    def reset(self):
        pass

    def set(self, storage_name):
        raise NotImplementedError

    def set_file_info(self, storage_name):
        """
        Path elements from the JSON listing:
        components
        components mfs
        components mfs i
        components mfs mfs
        components mfs w
        components survey
        components survey i
        components survey q
        components survey u
        components survey w
        :param storage_name str JSON output from rclone lsjson command
        """
        self._logger.debug('Begin set_file_info with rclone lsjson output')
        content = json.loads(storage_name)
        for entry in content:
            name = entry.get('Name')
            # if name.startswith('PSM') or '.fits' in name:
            if '.fits' in name:
                # keys are destination URIs
                try:
                    possum_name = PossumName(name)
                except CadcException as e:
                    self._logger.error(e)
                    self._logger.debug(traceback.format_exc())
                    continue
                if possum_name.file_uri not in self._file_info:
                    self._logger.debug(f'Retrieve FileInfo for {possum_name.file_uri}')
                    self._file_info[possum_name.file_uri] = FileInfo(
                        id=possum_name.file_uri,
                        file_type=get_file_type(name),
                        size=entry.get('Size'),
                        lastmod=make_datetime(entry.get('ModTime')),
                    )
                    if self._max_dt:
                        self._max_dt = max(self._file_info[possum_name.file_uri].lastmod, self._max_dt)
                    else:
                        self._max_dt = self._file_info[possum_name.file_uri].lastmod
                    self._storage_names[possum_name.file_uri] = possum_name
        self._logger.debug(f'End set_file_info with max datetime {self._max_dt}')

    def set_headers(self, storage_name, fqn):
        self._logger.debug(f'Begin set_headers for {storage_name.file_name}')
        for entry in storage_name.destination_uris:
            if entry not in self._headers and os.path.basename(entry) == os.path.basename(fqn):
                self._logger.debug(f'Retrieve headers for {entry}')
                self._retrieve_headers(entry, fqn)
        self._logger.debug('End set_headers')


class TodoMetadataReader(FileMetadataReader):
    def __init__(self, remote_metadata_reader):
        super().__init__()
        self._remote_metadata_reader = remote_metadata_reader

    def _retrieve_file_info(self, key, source_name):
        for storage_name in self._remote_metadata_reader.storage_names.values():
            for file_name in storage_name.stage_names:
                if key.endswith(file_name):
                    # the stage_names are the renamed values
                    self._logger.debug(f'Found remote FileInfo entry for {file_name} in {storage_name.file_uri}')
                    self._file_info[key] = self._remote_metadata_reader.file_info.get(storage_name.file_uri)
                    # missing md5sum - TODO - maybe rclone can report this
                    md5_sum = compute_md5sum(source_name)
                    self._file_info[key].md5sum = f'md5:{md5_sum}'


class RemoteIncrementalDataSource(IncrementalDataSource):

    def __init__(self, config, start_key, metadata_reader, **kwargs):
        super().__init__(config, start_key)
        self._data_source_extensions = config.lookup.get('rclone_include_pattern')
        self._metadata_reader = metadata_reader
        self._kwargs = kwargs
        # adjust config file syntax for the data sources, which was done so it would work with basic yaml
        if 'pawsey' in start_key:
            self._remote_key = start_key.replace('/', ':', 1)
        else:
            self._remote_key = start_key

    def _capture_todo(self):
        self._reporter.capture_todo(len(self._metadata_reader.file_info), self._rejected_files, self._skipped_files)
        # do not need the record of the rejected or skipped files any longer
        self._rejected_files = 0
        self._skipped_files = 0

    def _initialize_end_dt(self):
        self._logger.debug('Begin _initialize_end_dt')
        end_timestamp = self._state.bookmarks.get(self._start_key).get('end_timestamp')
        if end_timestamp is None:
            output = exec_cmd_info(f'rclone lsjson {self._remote_key} --recursive --max-age={self._start_dt.isoformat()} --include={self._data_source_extensions}')
        else:
            output = exec_cmd_info(f'rclone lsjson {self._remote_key} --recursive --max-age={self._start_dt.isoformat()} --min-age={end_timestamp.isoformat()} --include={self._data_source_extensions}')

        self._metadata_reader.set_file_info(output)
        if self._metadata_reader.max_dt:
            self._end_dt = self._metadata_reader.max_dt
        else:
            if end_timestamp:
                self._end_dt = make_datetime(end_timestamp)
            else:
                self._end_dt = datetime.now()
        self._capture_todo()
        self._logger.debug(f'End _initialize_end_dt with {self._end_dt}')

    def get_time_box_work(self, prev_exec_dt, exec_dt):
        self._logger.debug('Begin get_time_box_work')
        self._kwargs['prev_exec_dt'] = prev_exec_dt
        self._kwargs['exec_dt'] = exec_dt
        self._kwargs['metadata_reader'] = self._metadata_reader
        execution_unit = ExecutionUnit(self._config, **self._kwargs)
        execution_unit.num_entries, execution_unit.entry_dt = self._metadata_reader.get_time_box_work_parameters(prev_exec_dt, exec_dt)
        if execution_unit.num_entries > 0:
            execution_unit.start()
            # get the files from the DataSource to the staging space
            # --max-age  -> only transfer files younger than this in s
            # --min-age -> only transfer files older than this in s
            exec_cmd(
                f'rclone copy {self._remote_key} {execution_unit.working_directory} --max-age={prev_exec_dt.isoformat()} '
                f'--min-age={exec_dt.isoformat()} --include={self._data_source_extensions}'
            )
        self._logger.debug('End get_time_box_work')
        return execution_unit


class RemoteListDirDataSource(ListDirSeparateDataSource):

    def __init__(self, config):
        super().__init__(config)
        self._num_entries = None

    @property
    def num_entries(self):
        return self._num_entries

    def _capture_todo(self):
        # do not update total record count, that's already been done in the RemoteIncrementalDataSource
        pass

    def get_work(self):
        result = super().get_work()
        self._num_entries = len(result)
        return result


class ExecutionUnitStateRunner(StateRunner):
    """
    This class brings together the mechanisms for identifying the time-boxed lists of work to be done (DataSource
    specializations), and the mechanisms for translating a unit of work into something that CaomExecute can work with.

    For retries, accumulate the retry-able entries in a single file for each time-box interval, for each data source.
    After all the incremental execution, attempt the retries.
    """

    def __init__(
        self,
        config,
        organizer,
        data_sources,
        observable,
        reporter,
    ):
        super().__init__(
            config=config,
            organizer=organizer,
            builder=None,
            data_sources=data_sources,
            metadata_reader=None,
            observable=observable,
            reporter=reporter,
        )

    def _process_data_source(self, data_source):
        """
        Uses an iterable with an instance of StateRunnerMeta.

        :return: 0 for success, -1 for failure
        """
        data_source.initialize_start_dt()
        data_source.initialize_end_dt()
        prev_exec_time = data_source.start_dt
        incremented = increment_time(prev_exec_time, self._config.interval)
        exec_time = min(incremented, data_source.end_dt)

        self._logger.info(f'Starting at {prev_exec_time}, ending at {data_source.end_dt}')
        result = 0
        if prev_exec_time == data_source.end_dt:
            self._logger.info(f'Start time is the same as end time {prev_exec_time}, stopping.')
            exec_time = prev_exec_time
        else:
            cumulative = 0
            result = 0
            while exec_time <= data_source.end_dt:
                self._logger.info(f'Processing {data_source.start_key} from {prev_exec_time} to {exec_time}')
                save_time = exec_time
                self._reporter.set_log_location(self._config)
                work = data_source.get_time_box_work(prev_exec_time, exec_time)
                if work.num_entries > 0:
                    try:
                        self._logger.info(f'Processing {work.num_entries} entries.')
                        result |= work.do()
                    finally:
                        work.stop()
                    save_time = min(work.entry_dt, exec_time)
                    self._record_retries()
                self._record_progress(work.num_entries, cumulative, prev_exec_time, save_time)
                data_source.save_start_dt(save_time)
                if exec_time == data_source.end_dt:
                    # the last interval will always have the exec time equal to the end time, which will fail the
                    # while check so leave after the last interval has been processed
                    #
                    # but the while <= check is required so that an interval smaller than exec_time -> end_time will
                    # get executed, so don't get rid of the '=' in the while loop comparison, just because this one
                    # exists
                    break
                prev_exec_time = exec_time
                new_time = increment_time(prev_exec_time, self._config.interval)
                exec_time = min(new_time, data_source.end_dt)
                cumulative += work.num_entries

        data_source.save_start_dt(exec_time)
        msg = f'Done for {data_source.start_key}, saved state is {exec_time}'
        self._logger.info('=' * len(msg))
        self._logger.info(msg)
        self._logger.info(f'{self._reporter.success} of {self._reporter.all} records processed correctly.')
        self._logger.info('=' * len(msg))
        self._logger.debug(f'End _process_data_source with result {result}')
        return result


class ExecutionUnit:
    """
    Could be:
    - 1 file
    - 1 rclone timebox
    - 1 group of files for horizontal scaling deployment

    Temporal Cohesion between logging setup/teardown and workspace setup/teardown.
    """

    def __init__(self, config, **kwargs):
        """
        :param root_directory str staging space location
        :param label str name of the execution unit. Should be unique and conform to posix directory naming standards.
        """
        self._log_fqn = None
        self._logging_level = None
        self._log_handler = None
        self._task_types = config.task_types
        self._config = config
        self._entry_dt = None
        self._clients = kwargs.get('clients')
        # self._data_source = kwargs.get('data_source')
        self._remote_metadata_reader = kwargs.get('metadata_reader')
        self._observable = kwargs.get('observable')
        self._reporter = kwargs.get('reporter')
        self._prev_exec_dt = kwargs.get('prev_exec_dt')
        self._exec_dt = kwargs.get('exec_dt')
        self._label = (
            f'{self._prev_exec_dt.isoformat().replace(":", "_").replace(".", "_")}_'
            f'{self._exec_dt.isoformat().replace(":", "_").replace(".", "_")}'
        )
        self._working_directory = os.path.join(config.working_directory, self._label)
        if config.log_to_file:
            if config.log_file_directory:
                self._log_fqn = os.path.join(config.log_file_directory, self._label)
            else:
                self._log_fqn = os.path.join(config.working_directory, self._label)
            self._logging_level = config.logging_level
        self._num_entries = None
        self._central_wavelengths = {}  # key is original ObservationID, value is central wavelength
        self._observations = {}  # key is original ObservationID, values are Observation instances
        self._local_metadata_reader = None
        self._logger = logging.getLogger(self.__class__.__name__)

    @property
    def entry_dt(self):
        return self._entry_dt

    @entry_dt.setter
    def entry_dt(self, value):
        self._entry_dt = value

    @property
    def label(self):
        return self._label

    @property
    def num_entries(self):
        return self._num_entries

    @num_entries.setter
    def num_entries(self, value):
        self._num_entries = value

    @property
    def working_directory(self):
        return self._working_directory

    def do(self):
        """Make the execution unit one time-boxed copy from the DataSource to staging space, followed by a TodoRunner
        pointed to the staging space, and using that staging space with use_local_files: True. """
        self._logger.info(f'Begin do for {self._num_entries} entries in {self._label}')
        self._rename()
        result = None
        # set a Config instance to use the staging space with 'use_local_files: True'
        todo_config = deepcopy(self._config)
        todo_config.use_local_files = True
        todo_config.data_sources = [self._working_directory]
        todo_config.recurse_data_sources = True
        self._logger.debug(f'do config for TodoRunner: {todo_config}')
        self._local_metadata_reader = TodoMetadataReader(self._remote_metadata_reader)
        organizer = OrganizeExecutes(
            todo_config,
            META_VISITORS,
            DATA_VISITORS,
            None,  # chooser
            Transfer(),
            CadcTransfer(self._clients.data_client),
            self._local_metadata_reader,
            self._clients,
            self._observable,
        )
        local_data_source = RemoteListDirDataSource(todo_config)
        local_data_source.reporter = self._reporter
        builder = EntryBuilder(PossumName)
        # start a TodoRunner with the new Config instance, data_source, and metadata_reader
        todo_runner = TodoRunner(
            todo_config,
            organizer,
            builder=builder,
            data_sources=[local_data_source],
            metadata_reader=self._local_metadata_reader,
            observable=self._observable,
            reporter=self._reporter,
        )
        result = todo_runner.run()
        if todo_config.cleanup_files_when_storing:
            result |= todo_runner.run_retry()

        if local_data_source.num_entries != self._num_entries:
            self._logger.error(
                f'Expected to process {self._num_entries} entries, but found {local_data_source.num_entries} entries.'
            )
            result = -1
        self._logger.debug(f'End do with result {result}')
        return result

    def start(self):
        self._set_up_file_logging()
        self._create_workspace()

    def stop(self):
        self._clean_up_workspace()
        self._unset_file_logging()

    def _create_workspace(self):
        """Create the working area if it does not already exist."""
        self._logger.debug(f'Create working directory {self._working_directory}')
        create_dir(self._working_directory)

    def _clean_up_workspace(self):
        """Remove a directory and all its contents. Only do this if there is not a 'SCRAPE' task type, since the
        point of scraping is to be able to look at the pipeline execution artefacts once the processing is done.
        """
        # if os.path.exists(self._working_directory) and TaskType.SCRAPE not in self._task_types and self._config.cleanup_files_when_storing:
        if os.path.exists(self._working_directory) and TaskType.SCRAPE not in self._task_types:
            entries = glob.glob('*', root_dir=self._working_directory, recursive=True)
            if (self._config.cleanup_files_when_storing and len(entries) > 0) or len(entries) == 0:
                shutil.rmtree(self._working_directory)
                self._logger.error(f'Removed working directory {self._working_directory} and contents.')
        self._logger.debug('End _clean_up_workspace')

    def _RADEC_hms_dms_to_string(self, c: SkyCoord):
        """
        Round SkyCoordinate "c" (RA,DEC) to a string of "{hhmm}{ddmm}" for "{ra}{dec}"
        with proper rounding
        """
        ### round RA ###
        ra_h, ra_m, ra_s = c.ra.hms
        # round minutes based on seconds
        ra_m = round(ra_m + (ra_s / 60.0))
        # round hours based on minutes
        if ra_m >= 60:
            ra_m = 0
            ra_h += 1
        # check hours boundary
        if ra_h >= 24:
            ra_h = 0
        # create RA hh mm string with zero padding
        ra_hh_mm = f"{int(ra_h):02d}{int(ra_m):02d}"

        ### round DEC ###
        dsign = "+" # rounding absolute dec and fixing sign after
        if c.dec < (0*units.degree):
            dsign = "-"
        dec_d, dec_m, dec_s = np.abs(c.dec.dms)
        # round minutes based on seconds
        dec_m = round(abs(dec_m) + abs(dec_s)/60.0) 
        # round degrees based on minutes
        if dec_m >= 60:
            dec_m = 0
            dec_d += 1
        # create DEC dd mm string with zero padding and the proper sign
        dec_dd_mm = f"{dsign}{int(dec_d):02d}{int(dec_m):02d}"

        # Create final string "hhmm-ddmm"
        RADEC = f"{ra_hh_mm}{dec_dd_mm}"
        return RADEC

    def _find_new_file_name(self, hdr, mfs):
    # def name(fitsimage, prefix, version="v1", mfs=False):
        """
        Algorithm from @Sebokolodi, @Cameron-Van-Eck, @ErikOsinga (github).
        Setting up the name to be used for tiles. The script reads the bmaj and stokes from the fits header. The
        rest of the parameters are flexible to change.

        fitsimage: tile image
        prefix   : prefix to use. E.g. PSM for full survey,
                PSM_pilot1 for POSSUM pilot 1
                PSM_pilot2 for POSSUM pilot 2
        tileID   : tile pixel (Healpix pixel)

        version  : version of the output product. Version 1 is v1, version is v2,
                and so forth.

        """

        self._logger.debug('Begin _find_new_file_name')

        # get bmaj.
        bmaj = round(hdr.get('BMAJ') * 3600.0)
        if bmaj:
            bmaj =  f'{bmaj:2d}asec'

        # extract stokes parameter. It can be in either the 3rd or fourth axis.

        if hdr.get('CTYPE3') == 'STOKES':
            stokes = hdr.get('CRVAL3')
            # if Stokes is axis 3, then frequency is axis 4.
            freq0 = hdr.get('CRVAL4')
            dfreq = hdr.get('CDELT4')
            n = hdr.get('NAXIS4')
            if n and n > 1:
                cenfreq = round((freq0 + (freq0 + (n - 1) * dfreq))/(2.0 * 1e6))
            else:
                cenfreq = round(freq0/1e6)


        elif hdr.get('CTYPE4') == 'STOKES':
            stokes = hdr.get('CRVAL4')
            # if Stokes is axis 4, then frequency is axis 3. If we have >4 axis, the script will fail.
            freq0 = hdr.get('CRVAL3')
            dfreq = hdr.get('CDELT3')
            n = hdr.get('NAXIS3')
            if n and n > 1:
                cenfreq = round((freq0 + (freq0 + (n - 1) * dfreq))/(2.0 * 1e6))
            else:
                cenfreq = round(freq0/1e6)

        else:
            self._logger.error('Cannot find Stokes axis on the 3rd/4th axis')
            return None

        cenfreq = f'{round(cenfreq)}MHz'

        # stokes I=1, Q=2, U=3 and 4=V
        if int(stokes) == 1:
            stokesid = 'i'

        elif int(stokes) == 2:
            stokesid = 'q'

        elif int(stokes) == 3:
            stokesid = 'u'

        elif int(stokes) == 4:
            stokesid = 'v'

        self._logger.info('Define healpix grid for nside 32')
        # define the healpix grid
        hp = HEALPix(nside=32, order='ring', frame='icrs')

        # read the image crpix1 and crpix2 to determine the tile ID, and coordinates in degrees.
        naxis = hdr.get('NAXIS1')
        cdelt = abs(hdr.get('CDELT1'))
        hpx_ref_hdr = self._reference_header(naxis=naxis, cdelt=cdelt)
        hpx_ref_wcs = WCS(hpx_ref_hdr)

        crpix1 = hdr.get('CRPIX1')
        crpix2 = hdr.get('CRPIX2')
        crval1, crval2 = hpx_ref_wcs.wcs_pix2world(-crpix1, -crpix2 , 0)
        tileID = hp.lonlat_to_healpix(crval1 * units.deg, crval2 * units.deg, return_offsets=False)
        tileID = tileID - 1 #shifts by 1.

        # extract the RA and DEC for a specific pixel
        center = hp.healpix_to_lonlat(tileID) * units.deg
        RA, DEC = center.value

        self._logger.info(f'Derived RA is {RA} degrees and DEC is {DEC} degrees')
        c = SkyCoord(ra=RA * units.degree, dec=DEC * units.degree, frame='icrs')
        # coordinate string as "hhmm-ddmm"
        hmdm = self._RADEC_hms_dms_to_string(c)

        if mfs:
            outname = (
                f'{self._config.lookup.get('rename_prefix')}_{cenfreq}_{bmaj}_{hmdm}_{tileID}_t0_{stokesid}_'
                f'{self._config.lookup.get('rename_version')}.fits'
            )
        else:
            outname = (
                f'{self._config.lookup.get('rename_prefix')}_{cenfreq}_{bmaj}_{hmdm}_{tileID}_{stokesid}_'
                f'{self._config.lookup.get('rename_version')}.fits'
            )
        return outname

    def _reference_header(self, naxis, cdelt):
        """
        This is important as it allows us to properly determine correct pixel central pixel anywhere within the grid.

        NB: We use this header to convert the crpix1/2 in the header to tile ID, then degrees.

        :param cdelt the pixel size of the image in the grid. Must be the same as the one used for tiling.
        :param naxis number of pixels within each axis.
        """
        d = {
            'SIMPLE': 'T',
            'BITPIX': -32,
            'NAXIS': 2,
            'NAXIS1': naxis,
            'NAXIS2': naxis,
            'EXTEND': 'F',
            'CRPIX1': (naxis/2.0),
            'CRPIX2': (naxis/2.0) + 0.5,
            'PC1_1': 0.70710677,
            'PC1_2': 0.70710677,
            'PC2_1': -0.70710677,
            'PC2_2': 0.70710677,
            'CDELT1': -1 * cdelt,
            'CDELT2': cdelt,
            'CTYPE1': 'RA---HPX',
            'CTYPE2': 'DEC--HPX',
            'CRVAL1': 0.,
            'CRVAL2': 0.,
            'PV2_1': 4,
            'PV2_2': 3,
        }
        return fits.Header(d)


    def _rename(self):
        """The files from Pawsey need to be renamed. Some of the metadata to rename the files is most easily found
        in the plane-level metadata that is calculated server-side.

        The sandbox POSSUM configuration calculates the plane-level metadata, but the production POSSUM configuration
        does not.

        The BINTABLE files require do not contain enough metadata to easily calculate plane-level metadata, so for
        those files, that must be calculated by this application. The position bounding box will be the HEALpix
        coordinates for n=32.
        """
        self._logger.debug('Begin _rename')
        work = glob.glob('**/*.fits', root_dir=self._working_directory, recursive=True)
        for file_name in work:
            self._logger.info(f'Working on {file_name}')
            found_storage_name = None
            for storage_name in self._remote_metadata_reader.storage_names.values():
                if storage_name.file_name == os.path.basename(file_name):
                    found_storage_name = storage_name
                    break

            original_fqn = os.path.join(self._working_directory, file_name)
            self._remote_metadata_reader.set_headers(found_storage_name, original_fqn)
            # TODO - not quite sure which header index to return :)
            headers = self._remote_metadata_reader.headers.get(found_storage_name.file_uri)
            if headers:
                renamed_file = self._find_new_file_name(headers[0], ('mfs' in found_storage_name.file_name))
                found_storage_name.set_staging_name(renamed_file)
                renamed_fqn = original_fqn.replace(os.path.basename(original_fqn), renamed_file)
                os.rename(original_fqn, renamed_fqn)
                self._logger.info(f'Renamed {original_fqn} to {renamed_fqn}.')
            else:
                self._logger.warning(f'Could not find headers for {file_name}')
        self._logger.debug('End _rename')

    def _set_up_file_logging(self):
        """Configure logging to a separate file for each execution unit.

        If log_to_file is set to False, don't create a separate log file for each entry, because the application
        should leave as small a logging trace as possible.
        """
        if self._log_fqn and self._logging_level:
            self._log_handler = logging.FileHandler(self._log_fqn)
            formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)-12s:%(lineno)d:%(message)s')
            self._log_handler.setLevel(self._logging_level)
            self._log_handler.setFormatter(formatter)
            logging.getLogger().addHandler(self._log_handler)

    def _unset_file_logging(self):
        """Turn off the logging to the separate file for each entry being
        processed."""
        if self._log_handler:
            logging.getLogger().removeHandler(self._log_handler)
            self._log_handler.flush()
            self._log_handler.close()


class ExecutionUnitOrganizeExecutes(OrganizeExecutes):
    """A class to do nothing except be "not None" when called."""

    def __init__(self):
        pass

    def choose(self):
        # do nothing for the over-arching StateRunner
        pass

    def do_one(self, _):
        raise NotImplementedError


def remote_execution():
    """When running remotely, do a time-boxed 2-stage execution:
    1. stage 1 - the work is to use rclone to retrieve files to a staging area
    2. stage 2 - with the files in the staging area, use the pipeline as usual to store the files and create and
                 store the CAOM2 records, thumbnails, and previews

    Stage 1 is controlled with the ExecutionUnitStateRunner.
    Stage 2 is controlled with a TodoRunner, that is created for every ExecutionUnitStateRunner time-box that brings
    over files.
    """
    logging.debug('Begin remote_execution')
    config = Config()
    config.get_executors()
    set_logging(config)
    observable = Observable(config)
    reporter = ExecutionReporter(config, observable)
    reporter.set_log_location(config)
    metadata_reader = RemoteMetadataReader()
    organizer = ExecutionUnitOrganizeExecutes()
    clients = RCloneClients(config)
    StorageName.collection = config.collection
    StorageName.preview_scheme = config.preview_scheme
    StorageName.scheme = config.scheme
    kwargs = {
        'clients': clients,
        'observable': observable,
        'reporter': reporter,
    }
    data_sources = []
    for entry in config.data_sources:
        data_source = RemoteIncrementalDataSource(
            config,
            entry,  # should look like "acacia_possum:pawsey0980" acacia_possum => rclone named config, pawsey0980 => root bucket
            metadata_reader,
            **kwargs,
        )
        data_source.reporter = reporter
        data_sources.append(data_source)
    runner = ExecutionUnitStateRunner(
        config,
        organizer,
        data_sources=data_sources,
        observable=observable,
        reporter=reporter,
    )
    result = runner.run()
    result |= runner.run_retry()
    runner.report()
    logging.debug('End remote_execution')
    return result

# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2020.                            (c) 2020.
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

"""
This module implements the ObsBlueprint mapping.

Temporal WCS:

Cameron Van Eck - 17-10-23 - At present, most of the tiles don't have any observation date in the headers. A few do,
but they are likely not accurate (they somehow survived mosaicking?). Strongly prefer not to have observation dates
attached to these data -- since the relationship between obs. date and files is complicated, users should consult
our metadata database to determine which files are relevant.
"""

import logging
import traceback

from datetime import datetime, timedelta
from urllib import parse as parse

from astropy_healpix import HEALPix
from astropy.io import fits

from caom2 import CalibrationLevel, DataProductType, MultiPolygon, ProductType, ReleaseType, Point
from caom2 import Polygon, Position, SegmentType, Vertex
from caom2utils.blueprints import _to_float
from caom2utils.wcs_parsers import FitsWcsParser
from caom2pipe.astro_composable import get_datetime_mjd
from caom2pipe import caom_composable as cc
from caom2pipe.client_composable import repo_create, repo_delete, repo_get
from caom2pipe.manage_composable import CadcException, TaskType, ValueRepairCache


__all__ = ['mapping_factory']


class PossumValueRepair(ValueRepairCache):
    VALUE_REPAIR = {
        'chunk.custom.axis.axis.cunit': {'rad / m2': 'rad/m**2'},
        'chunk.custom.axis.axis.ctype': {'FDEP': 'FARADAY'},
    }

    def __init__(self):
        self._value_repair = PossumValueRepair.VALUE_REPAIR
        self._key = None
        self._values = None
        self._logger = logging.getLogger(self.__class__.__name__)


class Possum1DMapping(cc.TelescopeMapping):
    value_repair = PossumValueRepair()

    def __init__(self, storage_name, headers, clients, observable, observation, config):
        super().__init__(storage_name, headers, clients, observable, observation, config)
        # Cameron Van Eck - 23-10-23
        # Set release date to be 12 months after ingest. That’s the current POSSUM policy: data goes public 12
        # months after being generated. It doesn't have to be particularly precise: date of ingest + increment year by 1
        self._1_year_after = datetime.now() + timedelta(days=365)
        self._config = config

    def accumulate_blueprint(self, bp):
        """Configure the telescope-specific ObsBlueprint at the CAOM model Observation level."""
        self._logger.debug('Begin accumulate_bp.')
        super().accumulate_blueprint(bp)
        # JW - 17-10-23 - Use ASKAP
        bp.set('Observation.instrument.name', 'ASKAP')
        bp.set('Observation.metaRelease', self._1_year_after)
        bp.set('Observation.proposal.id', '_get_proposal_id()')
        bp.set_default('Observation.telescope.name', 'ASKAP')
        bp.set_default('Observation.telescope.geoLocationX', -2558266.717765)
        bp.set_default('Observation.telescope.geoLocationY', 5095672.176508)
        bp.set_default('Observation.telescope.geoLocationZ', -2849020.838078)
        bp.set('Plane.calibrationLevel', CalibrationLevel.CALIBRATED)
        bp.set('Plane.dataProductType', '_get_data_product_type()')
        bp.set('Plane.metaRelease', self._1_year_after)
        bp.set('Plane.dataRelease', self._1_year_after)
        bp.set('Plane.provenance.reference', 'https://askap.org/possum/')
        bp.set('Artifact.productType', ProductType.SCIENCE)
        bp.set('Artifact.releaseType', ReleaseType.DATA)
        self._logger.debug('Done accumulate_bp.')

    def update(self, file_info):
        """Called to fill multiple CAOM model elements and/or attributes
        (an n:n relationship between TDM attributes and CAOM attributes).
        """
        self._logger.debug(f'Begin update for {self._observation.observation_id}.')
        try:
            super().update(file_info)
            Possum1DMapping.value_repair.repair(self._observation)

            # the super call removes empty Parts before sending the Observation for server-side computing here
            for plane in self._observation.planes.values():
                if plane.product_id == self._storage_name.product_id:
                    self._post_plane_update(plane)

            self._logger.debug('Done update.')
            return self._observation
        except CadcException as e:
            tb = traceback.format_exc()
            self._logger.debug(tb)
            self._logger.error(f'Terminating ingestion for {self._observation.observation_id}')
            self._logger.error(e)
            return None

    def _get_data_product_type(self, ext):
        naxis = self._headers[ext].get('NAXIS')
        result = DataProductType.CUBE
        if naxis == 0:
            result = DataProductType.MEASUREMENTS
        elif naxis == 2:
            result = DataProductType.IMAGE
        elif naxis == 4:
            naxis3 = self._headers[ext].get('NAXIS3')
            naxis4 = self._headers[ext].get('NAXIS4')
            if naxis3 == 1 and naxis4 == 1:
                result = DataProductType.IMAGE
        return result

    def _get_position_resolution(self, ext):
        result = None
        # JW - 17-10-23 - Use either BMAJ or BMIN
        # Cameron Van Eck - 19-10-23 - Prefer BMAJ
        bmaj = self._headers[ext].get('BMAJ')
        if bmaj:
            # Cameron Van Eck - 23-10-23
            # FITS header value is in degrees, convert to arcseconds
            result = bmaj * 3600.0
        return result

    def _get_proposal_id(self, ext):
        # Cameron Van Eck - 23-10-23
        # For proposalID: All pilot data can have value “AS103". All full-survey data will have value “AS203”.
        result = 'AS203'
        if '_pilot' in self._storage_name.file_name:
            result = 'AS103'
        return result

    def _post_plane_update(self, plane):
        if TaskType.SCRAPE in self._config.task_types:
            self._logger.warning(f'No plane metadata update for {self._observation.observation_id}')
        else:
            # write the observation to the client which is configured for server-side metadata creation at the plane
            # level read the computed metadata from that CAOM service and copy the Plane-level bits
            try:
                repo_delete(self._clients.server_side_ctor_client, self._observation.collection, self._observation.observation_id, self._observable.metrics)
            except CadcException as e:
                # ignore delete failures as it's most likely a Not Found exception
                pass
            repo_create(self._clients.server_side_ctor_client, self._observation, self._observable.metrics)
            server_side_observation = repo_get(
                self._clients.server_side_ctor_client,
                self._storage_name.collection,
                self._storage_name.obs_id,
                self._observable.metrics,
            )
            for computed_plane in server_side_observation.planes.values():
                if computed_plane.product_id == plane.product_id:
                    # a reference will suffice for the copy as there's no _id field for the Plane-level attributes
                    self._logger.debug(f'Copying computed plane information from {plane.product_id}')
                    plane.custom = computed_plane.custom
                    plane.energy = computed_plane.energy
                    plane.observable = computed_plane.observable
                    plane.polarization = computed_plane.polarization
                    plane.position = computed_plane.position
                    plane.time = computed_plane.time

                    # do not clean up the Part, Chunk information, because it's used for cutout support

    def _update_artifact(self, artifact):
        delete_these = []
        for part in artifact.parts.values():
            if len(part.chunks) == 0:
                delete_these.append(part.name)
            else:
                for chunk in part.chunks:
                    if (
                        chunk.custom is None
                        and chunk.energy is None
                        and chunk.observable is None
                        and chunk.polarization is None
                        and chunk.position is None
                        and chunk.time is None
                    ) or (  # handle the Taylor BINTABLE extension case
                        chunk.custom is None
                        and chunk.energy is None
                        and chunk.observable is None
                        and chunk.polarization is None
                        and chunk.position is None
                        and chunk.time is not None
                    ):
                        delete_these.append(part.name)
                        break

        for entry in delete_these:
            artifact.parts.pop(entry)
            self._logger.info(f'Deleting part {entry} from artifact {artifact.uri}')

    @staticmethod
    def _from_pc_to_cd(from_header, to_header):
        cd1_1 = from_header.get('CD1_1')
        # caom2IngestSitelle.py, l745
        # CW
        # Be able to handle any of the 3 wcs systems used
        if cd1_1 is None:
            pc1_1 = from_header.get('PC1_1')
            if pc1_1 is not None:
                cdelt1 = _to_float(from_header.get('CDELT1'))
                if cdelt1 is None:
                    cd1_1 = _to_float(from_header.get('PC1_1'))
                    cd1_2 = _to_float(from_header.get('PC1_2'))
                    cd2_1 = _to_float(from_header.get('PC2_1'))
                    cd2_2 = _to_float(from_header.get('PC2_2'))
                else:
                    cdelt2 = _to_float(from_header.get('CDELT2'))
                    cd1_1 = cdelt1 * _to_float(from_header.get('PC1_1'))
                    cd1_2 = cdelt1 * _to_float(from_header.get('PC1_2'))
                    cd2_1 = cdelt2 * _to_float(from_header.get('PC2_1'))
                    cd2_2 = cdelt2 * _to_float(from_header.get('PC2_2'))
                to_header['CD1_1'] = cd1_1
                to_header['CD1_2'] = cd1_2
                to_header['CD2_1'] = cd2_1
                to_header['CD2_2'] = cd2_2


class SpatialMapping(Possum1DMapping):
    def __init__(self, storage_name, headers, clients, observable, observation, config):
        super().__init__(storage_name, headers, clients, observable, observation, config)

    def accumulate_blueprint(self, bp):
        """Configure the telescope-specific ObsBlueprint at the CAOM model
        Observation level."""
        self._logger.debug('Begin accumulate_bp.')
        super().accumulate_blueprint(bp)

        bp.set('Plane.provenance.name', 'POSSUM')
        bp.clear('Plane.provenance.lastExecuted')
        bp.add_attribute('Plane.provenance.lastExecuted', 'DATE')

        bp.configure_position_axes((1, 2))
        bp.set('Chunk.position.resolution', '_get_position_resolution()')

        self._logger.debug('Done accumulate_bp.')

    def _update_artifact(self, artifact):
        self._logger.debug(f'Begin _update_artifact for {artifact.uri}')
        super()._update_artifact(artifact)
        for part in artifact.parts.values():
            for chunk in part.chunks:
                if chunk.energy is not None:
                    # JW - 17-10-23 - remove restfrq
                    chunk.energy.restfrq = None
                self._update_chunk_position(chunk)
        self._logger.debug('End _update_artifact')

    def _update_chunk_position(self, chunk):
        self._logger.debug(f'Begin update_position_function for {self._storage_name.obs_id}')
        if chunk.position is not None:
            header = self._headers[0]
            cd1_1 = header.get('CD1_1')
            if cd1_1 is None:
                hdr = fits.Header()
                Possum1DMapping._from_pc_to_cd(header, hdr)
                for kw in [
                    'CDELT1',
                    'CDELT2',
                    'CRPIX1',
                    'CRPIX2',
                    'CRVAL1',
                    'CRVAL2',
                    'CTYPE1',
                    'CTYPE2',
                    'CUNIT1',
                    'CUNIT2',
                    'NAXIS',
                    'NAXIS1',
                    'NAXIS2',
                    'DATE-OBS',
                    'EQUINOX',
                ]:
                    hdr[kw] = header.get(kw)
                wcs_parser = FitsWcsParser(hdr, self._storage_name.obs_id, 0)
                wcs_parser.augment_position(chunk)
        self._logger.debug(f'End update_function_position for {self._storage_name.obs_id}')


class InputTileMapping(SpatialMapping):
    def __init__(self, storage_name, headers, clients, observable, observation, config):
        super().__init__(storage_name, headers, clients, observable, observation, config)

    def accumulate_blueprint(self, bp):
        """Configure the telescope-specific ObsBlueprint at the CAOM model
        Observation level."""
        self._logger.debug('Begin accumulate_bp.')
        super().accumulate_blueprint(bp)
        bp.set('Plane.calibrationLevel', CalibrationLevel.CALIBRATED)
        bp.clear('Plane.provenance.name')
        bp.add_attribute('Plane.provenance.name', 'ORIGIN')
        # JW - 17-10-23 - Use AusSRC for producer
        bp.set('Plane.provenance.producer', 'AusSRC')
        bp.set_default('Plane.provenance.reference', 'https://possum-survey.org/')
        bp.add_attribute('Plane.provenance.lastExecuted', 'DATE')
        bp.set_default('Plane.provenance.project', 'POSSUM')
        bp.configure_position_axes((1, 2))
        bp.set('Chunk.position.resolution', '_get_position_resolution()')

        bp.configure_energy_axis(3)
        bp.set_default('Chunk.energy.specsys', 'TOPOCENT')

        bp.configure_polarization_axis(4)
        self._logger.debug('Done accumulate_bp.')


class TaylorMapping(InputTileMapping):
    def __init__(self, storage_name, headers, clients, observable, observation, config):
        super().__init__(storage_name, headers, clients, observable, observation, config)

    def accumulate_blueprint(self, bp):
        """Configure the telescope-specific ObsBlueprint at the CAOM model
        Observation level."""
        self._logger.debug('Begin accumulate_bp.')
        super().accumulate_blueprint(bp)
        bp.set('Plane.provenance.name', '_get_plane_provenance_name()')
        bp.set('Plane.provenance.version', '_get_plane_provenance_version()')

        bp.configure_time_axis(5)
        bp.set('Chunk.time.axis.axis.ctype', 'TIME')
        bp.set('Chunk.time.axis.axis.cunit', 'd')
        bp.set('Chunk.time.axis.function.naxis', 1)
        bp.set('Chunk.time.axis.function.refCoord.pix', 0.5)
        bp.set('Chunk.time.axis.function.refCoord.val', '_get_time_function_refcoord_val()')

        self._logger.debug('Done accumulate_bp.')

    def _get_plane_provenance_name(self, ext):
        origin = self._headers[ext].get('ORIGIN')
        result = None
        if origin:
            result = origin
            bits = origin.split(' ')
            if len(bits) >= 3:
                other_bits = bits[2].split(':')
                result = f'{bits[0]} {other_bits[0]}'
        return result

    def _get_plane_provenance_version(self, ext):
        origin = self._headers[ext].get('ORIGIN')
        result = None
        if origin:
            bits = origin.split(' ')
            if len(bits) >= 3:
                other_bits = bits[2].split(':')
                result = f'{bits[1]} {other_bits[1]}'
        return result

    def _get_time_function_refcoord_val(self, ext):
        date_obs = self._headers[ext].get('DATE-OBS')
        if date_obs is not None:
            result = get_datetime_mjd(date_obs)
        return result

    def _update_artifact(self, artifact):
        super()._update_artifact(artifact)
        for part in artifact.parts.values():
            for chunk in part.chunks:
                if chunk.time_axis is not None:
                    chunk.time_axis = None


class OutputSpatial(SpatialMapping):
    def __init__(self, storage_name, headers, clients, observable, observation, config):
        super().__init__(storage_name, headers, clients, observable, observation, config)

    def accumulate_blueprint(self, bp):
        """Configure the telescope-specific ObsBlueprint at the CAOM model
        Observation level."""
        self._logger.debug('Begin accumulate_bp.')
        super().accumulate_blueprint(bp)

        bp.set('Plane.provenance.name', 'POSSUM')
        bp.clear('Plane.provenance.lastExecuted')
        bp.add_attribute('Plane.provenance.lastExecuted', 'DATE')

        bp.configure_position_axes((1, 2))
        bp.set('Chunk.position.resolution', '_get_position_resolution()')

        self._logger.debug('Done accumulate_bp.')


class Output3DMapping(OutputSpatial):
    def __init__(self, storage_name, headers, clients, observable, observation, config):
        super().__init__(storage_name, headers, clients, observable, observation, config)

    def accumulate_blueprint(self, bp):
        """Configure the telescope-specific ObsBlueprint at the CAOM model
        Observation level."""
        super().accumulate_blueprint(bp)
        bp.configure_polarization_axis(3)
        bp.configure_custom_axis(4)
        self._logger.debug('Done accumulate_bp.')


class OutputCustomSpatial(OutputSpatial):
    def __init__(self, storage_name, headers, clients, observable, observation, config):
        super().__init__(storage_name, headers, clients, observable, observation, config)

    def accumulate_blueprint(self, bp):
        """Configure the telescope-specific ObsBlueprint at the CAOM model
        Observation level."""
        super().accumulate_blueprint(bp)
        bp.configure_custom_axis(3)
        self._logger.debug('Done accumulate_bp.')


class OutputFWHM(OutputCustomSpatial):
    def __init__(self, storage_name, headers, clients, observable, observation, config):
        super().__init__(storage_name, headers, clients, observable, observation, config)

    def accumulate_blueprint(self, bp):
        """Configure the telescope-specific ObsBlueprint at the CAOM model
        Observation level."""
        super().accumulate_blueprint(bp)
        bp.configure_polarization_axis(4)
        self._logger.debug('Done accumulate_bp.')


class Pilot1OutputSpatial(Possum1DMapping):
    def __init__(self, storage_name, headers, clients, observable, observation, config):
        super().__init__(storage_name, headers, clients, observable, observation, config)

    def accumulate_blueprint(self, bp):
        """Configure the telescope-specific ObsBlueprint at the CAOM model
        Observation level."""
        self._logger.debug('Begin accumulate_bp.')
        super().accumulate_blueprint(bp)

        bp.set('Plane.provenance.name', 'POSSUM')
        bp.clear('Plane.provenance.lastExecuted')
        bp.add_attribute('Plane.provenance.lastExecuted', 'DATE')

        bp.configure_position_axes((1, 2))
        bp.clear('Chunk.position.axis.function.cd11')
        bp.clear('Chunk.position.axis.function.cd22')
        bp.add_attribute('Chunk.position.axis.function.cd11', 'CDELT1')
        bp.set('Chunk.position.axis.function.cd12', 0.0)
        bp.set('Chunk.position.axis.function.cd21', 0.0)
        bp.add_attribute('Chunk.position.axis.function.cd22', 'CDELT2')
        bp.set('Chunk.position.resolution', '_get_position_resolution()')

        self._logger.debug('Done accumulate_bp.')

    def update(self, file_info):
        """Called to fill multiple CAOM model elements and/or attributes
        (an n:n relationship between TDM attributes and CAOM attributes).
        """
        super().update(file_info)
        for plane in self._observation.planes.values():
            for artifact in plane.artifacts.values():
                if artifact.uri != self._storage_name.file_uri:
                    continue
                for part in artifact.parts.values():
                    for chunk in part.chunks:
                        if chunk.energy is not None:
                            chunk.energy_axis = None
        return self._observation


class Catalog1DMapping(Possum1DMapping):

    def __init__(self, storage_name, headers, clients, observable, observation, config):
        super().__init__(storage_name, headers, clients, observable, observation, config)

    def accumulate_blueprint(self, bp):
        super().accumulate_blueprint(bp)
        bp.set('DerivedObservation.members', {})
        bp.set('Observation.algorithm.name', 'catalog')
        bp.set('Plane.calibrationLevel', CalibrationLevel.PRODUCT)
        bp.set('Plane.dataProductType', DataProductType.CATALOG)

    def _post_plane_update(self, plane):
        pass

    def _update_plane(self, plane):
        self._logger.debug(f'Begin _update_plane for {plane.product_id}')
        super()._update_plane(plane)
        hp = HEALPix(nside=32, order='ring', frame='icrs')
        vertices = []
        points = []
        vertex_start = None
        # TODO - the healpix index is shifted by 1 in the rename, does it need to be unshifted
        # by 1 here?
        x = hp.boundaries_skycoord(healpix_index=self._storage_name.healpix_index, step=1)
        for x1 in x:
            for x2 in x1:
                point = Point(x2.ra.value, x2.dec.value)
                points.append(point)
                if vertex_start:
                    vertex = Vertex(x2.ra.value, x2.dec.value, SegmentType.LINE)
                else:
                    vertex = Vertex(x2.ra.value, x2.dec.value, SegmentType.MOVE)
                    vertex_start = vertex
                vertices.append(vertex)
        vertex_end = Vertex(vertex_start.cval1, vertex_start.cval2, SegmentType.CLOSE)
        vertices.append(vertex_end)
        samples = MultiPolygon(vertices=vertices)
        bounds = Polygon(points=points, samples=samples)
        position = Position(
            bounds=bounds, resolution=self._storage_name.spatial_resolution
        )
        plane.position = position
        self._logger.debug('End _update_plane')


def mapping_factory(storage_name, headers, clients, observable, observation, config):
    if storage_name.is_bintable:
        result = Catalog1DMapping(storage_name, headers, clients, observable, observation, config)
    elif storage_name.product_id == '3d_pipeline':
        naxis = None
        if headers and len(headers) > 0:
            naxis = headers[0].get('NAXIS')
        if '_FWHM' in storage_name.file_name:
            if naxis:
                if naxis == 3:
                    result = OutputCustomSpatial(storage_name, headers, clients, observable, observation, config)
                elif naxis == 4:
                    result = OutputFWHM(storage_name, headers, clients, observable, observation, config)
                else:
                    raise CadcException(f'No mapping for {storage_name.file_name}.')
        else:
            if naxis and naxis == 2:
                if 'pilot' in storage_name.file_name:
                    result = Pilot1OutputSpatial(storage_name, headers, clients, observable, observation, config)
                else:
                    result = OutputSpatial(storage_name, headers, clients, observable, observation, config)
            else:
                result = Output3DMapping(storage_name, headers, clients, observable, observation, config)
    elif storage_name.product_id.startswith('multifrequencysynthesis_'):
        result = TaylorMapping(storage_name, headers, clients, observable, observation, config)
    else:
        result = InputTileMapping(storage_name, headers, clients, observable, observation, config)
    logging.debug(f'Constructed {result.__class__.__name__} for mapping {storage_name.file_name}.')
    return result

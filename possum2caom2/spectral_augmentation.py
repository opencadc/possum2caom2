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
import numpy as np
from astropy.io import fits
from astropy.table import Table
from caom2 import Energy, EnergyBand, Interval, shape
from caom2pipe.astro_composable import from_file_units_to_m
from caom2pipe.manage_composable import CadcException


def visit(observation, **kwargs):
    logging.info( f'Begin spectral bounds augmentation for {observation.observation_id}')
    storage_name = kwargs.get('storage_name')
    if storage_name is None:
        raise CadcException('Require a storage name.')
    else:
        if storage_name.product_id != '1d_pipeline':
            return observation

    count = 0
    for plane in observation.planes.values():
        if plane.product_id != storage_name.product_id:
            continue
        with fits.open(storage_name.source_names[0], memmap=True) as hdul:
            if len(hdul) > 1 and 'freq' in hdul[1].columns.names:
                # convert 'freq' from object to float
                data = np.array(Table(hdul[1].data)['freq'][1])
                min_bound = data.min()
                max_bound = data.max()
                # convert from file units to 'm'
                convert_from = hdul[1].columns['freq'].unit
                if convert_from == 'Hz':
                    max_bound_m = from_file_units_to_m(convert_from, min_bound)
                    min_bound_m = from_file_units_to_m(convert_from, max_bound)
                    energy = Energy()
                    energy.em_band = EnergyBand.RADIO
                    energy.dimension = 1
                    sample = shape.SubInterval(min_bound_m, max_bound_m)
                    energy.bounds = Interval(min_bound_m, max_bound_m, samples=[sample])
                    plane.energy = energy
                    count += 1
                else:
                    logging.warning(f'Unexpected units {convert_from} in {storage_name.file_name}')
    logging.info( f'End spectral bounds augmentation for {observation.observation_id} with {count} change(s).')
    return observation

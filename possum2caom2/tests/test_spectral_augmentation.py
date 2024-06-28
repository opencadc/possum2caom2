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

from glob import glob
from os.path import basename

from caom2pipe.manage_composable import read_obs_from_file
from possum2caom2.spectral_augmentation import visit
from possum2caom2.storage_name import PossumName


def pytest_generate_tests(metafunc):
    test_data_dir = f'{metafunc.config.invocation_dir}/data'
    obs_id_list = glob(f'{test_data_dir}/spectral_visit/*.fits')
    metafunc.parametrize('test_name', obs_id_list)


def test_visit(test_name, test_data_dir):
    test_storage_name = PossumName(test_name)
    kwargs = {
        'storage_name': test_storage_name,
    }
    test_observation = read_obs_from_file(
        f'{test_data_dir}/possum/{basename(test_name).replace(".fits", ".expected.xml")}'
    )
    test_observation = visit(test_observation, **kwargs)
    if test_name.endswith('_spectra.fits'):
        test_energy = test_observation.planes['1d_pipeline'].energy
        assert test_energy is not None, 'expect energy'
        assert len(test_energy.bounds.samples) == 1, f'wrong sample count {len(test_energy.bounds.samples)}'
        test_sample = test_energy.bounds.samples[0]
        assert test_sample.lower == 0.20833522378721048, f'wrong minimum {test_sample.lower}'
        assert test_sample.upper == 0.2313229937341911, f'wrong maximum {test_sample.upper}'
    else:
        test_energy = test_observation.planes['1d_pipeline'].energy
        # the em_band has a value!
        assert test_energy is None, f'do not expect energy {test_name}\n{test_energy}'

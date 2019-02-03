#!/usr/bin/env python

from os.path import exists

from xapi.storage.libs.xcpng.datapath import DATAPATHES, Implementation
from xapi.storage.libs.xcpng.datapath import QdiskDatapath as _QdiskDatapath_
from xapi.storage.libs.xcpng.libsbd.qemudisk import Qemudisk
from xapi.storage.libs.xcpng.libsbd.meta import MetadataHandler
from xapi.storage.libs.xcpng.utils import call, _call, SR_PATH_PREFIX, VDI_PREFIXES, get_vdi_type_by_uri, \
                                          get_sr_uuid_by_uri
from xapi.storage.libs.xcpng.libsbd.sbd_utils import get_sheep_port

NBDS_MAX = 32

class QdiskDatapath(_QdiskDatapath_):

    def __init__(self):
        super(QdiskDatapath, self).__init__()
        self.MetadataHandler = MetadataHandler()
        self.qemudisk = Qemudisk

    def _is_nbd_device_connected(self, dbg, nbd_device):
        call(dbg, ['/usr/sbin/modprobe', 'nbd', "nbds_max=%s" % NBDS_MAX])
        if not exists(nbd_device):
            raise Exception('There are no more free NBD devices')
        returncode = _call(dbg, ['/usr/sbin/nbd-client', '-check', nbd_device])
        if returncode == 0:
            return True
        if returncode == 1:
            return False

    def _find_unused_nbd_device(self, dbg):
        for device_no in range(0, NBDS_MAX):
            nbd_device = "/dev/nbd{}".format(device_no)
            if not self._is_nbd_device_connected(dbg, nbd_device=nbd_device):
                return nbd_device

    def map_vol(self, dbg, uri, chained=False):
        if not chained:
            nbd_dev = self._find_unused_nbd_device(dbg)
            image_meta = self.MetadataHandler.load(dbg, uri)
            call(dbg, ['/lib64/qemu-dp/bin/qemu-nbd',
                       '-c', nbd_dev,
                       '-f', 'raw',
                       "sheepdog+unix:///%s%s?socket=%s" % (VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)],
                                                           image_meta['dog_vdi_uuid'],
                                                           "%s/%s/sock" % (SR_PATH_PREFIX, get_sr_uuid_by_uri(dbg, uri)))])
            image_meta = {'nbd_dev': nbd_dev}
            self.MetadataHandler.update(dbg, uri, image_meta)
            self.blkdev = nbd_dev

            super(QdiskDatapath, self).map_vol(dbg, uri, chained=False)

    def unmap_vol(self, dbg, uri, chained=False):
        if not chained:
            super(QdiskDatapath, self).unmap_vol(dbg, uri, chained=False)
            image_meta = self.MetadataHandler.load(dbg, uri)
            call(dbg, ['/lib64/qemu-dp/bin/qemu-nbd', '-d', image_meta['nbd_dev']])
            image_meta = {'nbd_dev': None}
            self.MetadataHandler.update(dbg, uri, image_meta)

    def gen_vol_uri(self, dbg, uri):
        image_meta = self.MetadataHandler.load(dbg, uri)
        return "sheepdog+unix:///%s%s?socket=%s" % (VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)],
                                                    image_meta['dog_vdi_uuid'],
                                                    "%s/%s/sock" % (SR_PATH_PREFIX, get_sr_uuid_by_uri(dbg, uri)))
        #return "sheepdog:127.0.0.1:%s:%s%s" % (get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)),
        #                                       VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)],
        #                                       image_meta['dog_vdi_uuid'])


DATAPATHES['qdisk'] = QdiskDatapath
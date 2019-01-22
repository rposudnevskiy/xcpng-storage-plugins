#!/usr/bin/env python

import re
from os import system

from xapi.storage.libs.xcpng.utils import VDI_PREFIXES, get_sr_uuid_by_uri, get_vdi_type_by_uri, get_vdi_uuid_by_uri, \
                                          get_image_name_by_uri
from xapi.storage.libs.xcpng.utils import roundup
from xapi.storage.libs.xcpng.volume import VolumeOperations, RAWVolume, QCOW2Volume, Implementation
from xapi.storage.libs.xcpng.libsbd.sbd_utils import dog_vdi_create, dog_vdi_delete, dog_vdi_resize, get_sheep_port, \
                                                     dog_vdi_list, VOLBLOCKSIZE
from xapi.storage.libs.xcpng.libsbd.meta import SBDMetadataHandler

from xapi.storage import log


class SBDVolumeOperations(VolumeOperations):

    def create(self, dbg, uri, size, path):
        dog_vdi_create(dbg,
                       get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)),
                       "%s%s" % (VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)], get_vdi_uuid_by_uri(dbg, uri)),
                       size)

    def destroy(cls, dbg, uri, path):
        dog_vdi_delete(dbg,
                       get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)),
                       "%s%s" % (VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)], get_vdi_uuid_by_uri(dbg, uri)))

    def resize(self, dbg, uri, new_size):
        dog_vdi_resize(dbg,
                       get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)),
                       "%s%s" % (VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)], get_vdi_uuid_by_uri(dbg, uri)),
                       new_size)

    def map(self, dbg, uri, path):
        system("/lib64/qemu-dp/bin/qemu-nbd -c /dev/nbd0 -f raw sheepdog:%s" % get_image_name_by_uri(dbg, uri))
        system("ln -s /dev/nbd0 %s" % path)

    def unmap(self, dbg, uri, path):
        system("unlink %s" % path)
        system("/lib64/qemu-dp/bin/qemu-nbd -d /dev/nbd0 >/dev/null 2>&1")

    def get_phisical_utilization(self, dbg, uri):
        regex = re.compile(get_image_name_by_uri(dbg, uri))
        for vdi in dog_vdi_list(dbg, get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri))):
            log.debug("%s: get_phisical_utilization: vdi: %s" % (dbg, vdi))
            result = regex.match(vdi[1])
            if result:
                retval = int(vdi[4])
                break
        return retval

    def roundup_size(self, dbg, size):
        return roundup(VOLBLOCKSIZE, size)


class SBDRAWVolume(RAWVolume):

    def __init__(self):
        super(SBDRAWVolume, self).__init__()
        self.MetadataHandler = SBDMetadataHandler
        self.VolOpsHendler = SBDVolumeOperations()


class SBDQCOW2Volume(QCOW2Volume):

    def __init__(self):
        super(SBDQCOW2Volume, self).__init__()
        self.MetadataHandler = SBDMetadataHandler
        self.VolOpsHendler = SBDVolumeOperations()


class SBDImplementation(Implementation):

    def __init__(self):
        super(SBDImplementation, self).__init__()
        self.RAWVolume = SBDRAWVolume()
        self.QCOW2Volume = SBDQCOW2Volume()

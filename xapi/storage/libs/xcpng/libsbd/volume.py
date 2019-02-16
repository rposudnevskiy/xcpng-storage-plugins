#!/usr/bin/env python

import uuid
from xapi.storage import log
from xapi.storage.libs.xcpng.utils import VDI_PREFIXES, get_sr_uuid_by_uri, get_vdi_type_by_uri
from xapi.storage.libs.xcpng.utils import roundup
from xapi.storage.libs.xcpng.volume import VolumeOperations as _VolumeOperations_
from xapi.storage.libs.xcpng.meta import IMAGE_UUID_TAG
from xapi.storage.libs.xcpng.libsbd.sbd_utils import dog_vdi_create, dog_vdi_delete, dog_vdi_resize, get_sheep_port, \
                                                     dog_vdi_info, VOLBLOCKSIZE


class VolumeOperations(_VolumeOperations_):

    def create(self, dbg, uri, size):
        volume_meta = self.MetadataHandler.get_vdi_meta(dbg, uri)
        dog_vdi_create(dbg,
                       get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)),
                       "%s%s" % (VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)], volume_meta[IMAGE_UUID_TAG]),
                       size)

    def destroy(self, dbg, uri):
        volume_meta = self.MetadataHandler.get_vdi_meta(dbg, uri)
        dog_vdi_delete(dbg,
                       get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)),
                       "%s%s" % (VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)], volume_meta[IMAGE_UUID_TAG]))

    def resize(self, dbg, uri, new_size):
        volume_meta = self.MetadataHandler.get_vdi_meta(dbg, uri)
        dog_vdi_resize(dbg,
                       get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)),
                       "%s%s" % (VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)], volume_meta[IMAGE_UUID_TAG]),
                       new_size)

    def get_phisical_utilization(self, dbg, uri):
        volume_meta = self.MetadataHandler.get_vdi_meta(dbg, uri)
        vdi_name = "%s%s" % (VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)], volume_meta[IMAGE_UUID_TAG])
        vdi_info = dog_vdi_info(dbg, get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)), vdi_name)
        log.debug("%s: get_phisical_utilization: uri: %s: utilization: %s" % (dbg, uri, vdi_info[4]))
        return int(vdi_info[4])

    def roundup_size(self, dbg, size):
        return roundup(VOLBLOCKSIZE, size)

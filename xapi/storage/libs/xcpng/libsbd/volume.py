#!/usr/bin/env python

import re
import uuid
from os import system

from xapi.storage.libs.xcpng.utils import VDI_PREFIXES, get_sr_uuid_by_uri, get_vdi_type_by_uri
from xapi.storage.libs.xcpng.utils import roundup

from xapi.storage.libs.xcpng.volume import VOLUME_TYPES, Implementation
from xapi.storage.libs.xcpng.volume import VolumeOperations as _VolumeOperations_
from xapi.storage.libs.xcpng.volume import RAWVolume as _RAWVolume_
from xapi.storage.libs.xcpng.volume import QCOW2Volume as _QCOW2Volume_

from xapi.storage.libs.xcpng.libsbd.sbd_utils import dog_vdi_create, dog_vdi_delete, dog_vdi_resize, get_sheep_port, \
                                                     dog_vdi_info, VOLBLOCKSIZE
from xapi.storage.libs.xcpng.libsbd.meta import MetadataHandler, DOG_VDI_UUID_TAG
from xapi.storage.libs.xcpng.libsbd.datapath import DATAPATHES

from xapi.storage import log


class VolumeOperations(_VolumeOperations_):

    def __init__(self):
        super(VolumeOperations, self).__init__()
        self.MetadataHandler = MetadataHandler()

    def create(self, dbg, uri, size):
        dog_uuid = str(uuid.uuid4())
        image_meta = {DOG_VDI_UUID_TAG: dog_uuid}
        self.MetadataHandler.update(dbg, uri, image_meta)
        dog_vdi_create(dbg,
                       get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)),
                       "%s%s" % (VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)], dog_uuid),
                       size)

    def destroy(self, dbg, uri):
        image_meta = self.MetadataHandler.load(dbg, uri)
        dog_vdi_delete(dbg,
                       get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)),
                       "%s%s" % (VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)], image_meta[DOG_VDI_UUID_TAG]))

    def resize(self, dbg, uri, new_size):
        image_meta = self.MetadataHandler.load(dbg, uri)
        dog_vdi_resize(dbg,
                       get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)),
                       "%s%s" % (VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)], image_meta[DOG_VDI_UUID_TAG]),
                       new_size)

    def swap(self, dbg, uri1, uri2):
        log.debug("%s: xcpng.libsbd.volume.VolumeOperations.swap: uri1: %s uri2: %s" % (dbg, uri1, uri2))
        image1_meta = self.MetadataHandler.load(dbg, uri1)
        image2_meta = self.MetadataHandler.load(dbg, uri2)
        dog_vdi_uuid1 = image1_meta[DOG_VDI_UUID_TAG]
        dog_vdi_uuid2 = image2_meta[DOG_VDI_UUID_TAG]
        image1_meta = {DOG_VDI_UUID_TAG: dog_vdi_uuid2}
        image2_meta = {DOG_VDI_UUID_TAG: dog_vdi_uuid1}
        self.MetadataHandler.update(dbg, uri1, image1_meta)
        self.MetadataHandler.update(dbg, uri2, image2_meta)

    def get_phisical_utilization(self, dbg, uri):
        image_meta = self.MetadataHandler.load(dbg, uri)
        vdi_name = "%s%s" % (VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)], image_meta[DOG_VDI_UUID_TAG])
        vdi_info = dog_vdi_info(dbg, get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)), vdi_name)
        log.debug("%s: get_phisical_utilization: uri: %s: utilization: %s" % (dbg, uri, vdi_info[4]))
        return int(vdi_info[4])

    def roundup_size(self, dbg, size):
        return roundup(VOLBLOCKSIZE, size)


class RAWVolume(_RAWVolume_):

    def __init__(self):
        super(RAWVolume, self).__init__()
        self.MetadataHandler = MetadataHandler()
        self.VolOpsHendler = VolumeOperations()
        self.Datapathes = {}
        for k, v in DATAPATHES.iteritems():
            self.Datapathes[k] = v()


class QCOW2Volume(_QCOW2Volume_):

    def __init__(self):
        super(QCOW2Volume, self).__init__()
        self.MetadataHandler = MetadataHandler()
        self.VolOpsHendler = VolumeOperations()
        self.Datapathes = {}
        for k, v in DATAPATHES.iteritems():
            self.Datapathes[k] = v()


VOLUME_TYPES['raw'] = RAWVolume
VOLUME_TYPES['qcow2'] = QCOW2Volume

#!/usr/bin/env python

from time import time
from json import dumps, loads
from struct import pack, unpack

from xapi.storage import log

from tinydb import TinyDB, where
from tinydb.storages import MemoryStorage

from xapi.storage.libs.xcpng.meta import MetadataHandler as _MetadataHandler_
from xapi.storage.libs.xcpng.meta import MetaDBOperations as _MetaDBOperations_
from xapi.storage.libs.xcpng.meta import UUID_TAG

from xapi.storage.libs.xcpng.utils import VDI_PREFIXES, get_sr_uuid_by_uri, get_vdi_type_by_uri, get_vdi_uuid_by_name
from xapi.storage.libs.xcpng.libsbd.sbd_utils import dog_vdi_create, dog_vdi_write, dog_vdi_read, dog_vdi_delete, \
                                                     get_sheep_port, dog_vdi_setattr, dog_vdi_delattr, dog_vdi_list

DOG_VDI_UUID_TAG = 'dog_vdi_uuid'


class MetaDBOperations(_MetaDBOperations_):

    def create(self, dbg, uri):
        log.debug("%s: xcpng.libsbd.meta.MetaDBOpeations.create: uri: %s" % (dbg, uri))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        sheep_port = get_sheep_port(dbg, sr_uuid)

        db = TinyDB(storage=MemoryStorage, default_table='sr')
        dog_vdi_create(dbg, sheep_port , '__meta__', '8M')
        data = dumps(db._storage.read())
        length = len(data)
        dog_vdi_write(dbg, sheep_port, '__meta__', pack("!I%ss" % length, length, data), 0, length+4)

    def destroy(self, dbg, uri):
        log.debug("%s: xcpng.libsbd.meta.MetaDBOpeations.destroy: uri: %s" % (dbg, uri))
        dog_vdi_delete(dbg, get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)), '__meta__')

    def load(self, dbg, uri):
        log.debug("%s: xcpng.libsbd.meta.MetaDBOpeations.load: uri: %s" % (dbg, uri))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        sheep_port = get_sheep_port(dbg, sr_uuid)

        db = TinyDB(storage=MemoryStorage, default_table='sr')
        length = unpack('!I', dog_vdi_read(dbg, sheep_port, '__meta__', 0, 4))[0]
        data = unpack('!%ss' % length, dog_vdi_read(dbg, sheep_port, '__meta__', 4, length))[0]
        db._storage.write(loads(data))

        return db

    def dump(self, dbg, uri, db):
        log.debug("%s: xcpng.libsbd.meta.MetaDBOpeations.dump: uri: %s" % (dbg, uri))

        data = dumps(db._storage.read())
        length = len(data)
        dog_vdi_write(dbg,
                      get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)),
                      '__meta__',
                      pack("!I%ss" % length, length, data),
                      0,
                      length + 4)

    def lock(self, dbg, uri, timeout=10):
        log.debug("%s: xcpng.libsbd.meta.MetaDBOpeations.lock: uri: %s" % (dbg, uri))

        start_time = time()

        while True:
            try:
                dog_vdi_setattr(dbg,
                                get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)),
                                '__meta__',
                                'locked',
                                'locked',
                                exclusive=True)
                break
            except Exception as e:
                if time() - start_time >= timeout:
                    log.debug("%s: xcpng.libsbd.meta.MetaDBOpeations.lock: Failed to lock MetaDB for uri: %s" % (dbg, uri))
                    raise Exception(e)
                pass

    def unlock(self, dbg, uri, timeout=10):
        log.debug("%s: xcpng.libsbd.meta.MetaDBOpeations.unlock: uri: %s" % (dbg, uri))

        try:
            dog_vdi_delattr(dbg, get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)), '__meta__', 'locked')
        except Exception as e:
            log.debug("%s: xcpng.libsbd.meta.MetaDBOpeations.unlock: Failed to unlock MetaDB for uri: %s" % (dbg, uri))
            raise Exception(e)

class MetadataHandler(_MetadataHandler_):

    def __init__(self):
        self.MetaDBOpsHendler = MetaDBOperations()

    def get_vdi_list(self, dbg, uri, vols):
        log.debug("%s: xcpng.meta.MetadataHandler.get_vdi_list: uri: %s" % (dbg, uri))

        vdis = []

        try:
            db = self.MetaDBOpsHendler.load(dbg, uri)
            table = db.table('vdis')
            for _vdi_ in vols:
                image_meta = table.search(where(DOG_VDI_UUID_TAG) == get_vdi_uuid_by_name(dbg, _vdi_))[0]
                if UUID_TAG in image_meta:
                    vdis.append(image_meta[UUID_TAG])
            return vdis
        except Exception as e:
            log.error("%s: xcpng.meta.MetadataHandler.get_vdi_chain: Failed to get vdi chain for uri: %s " % (dbg, uri))
            raise Exception(e)

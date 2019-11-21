#!/usr/bin/env python

from time import time
from struct import pack, unpack
from xapi.storage import log
from xapi.storage.libs.xcpng.meta import MetaDBOperations as _MetaDBOperations_
from xapi.storage.libs.xcpng.utils import get_sr_uuid_by_uri
from xapi.storage.libs.xcpng.libsbd.sbd_utils import dog_vdi_create, dog_vdi_write, dog_vdi_read, dog_vdi_delete, \
                                                     get_sheep_port, dog_vdi_setattr, dog_vdi_delattr


class MetaDBOperations(_MetaDBOperations_):

    def create(self, dbg, uri):
        log.debug("%s: xcpng.libsbd.meta.MetaDBOpeations.create: uri: %s" % (dbg, uri))

        data = '{"sr": {}}'
        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        sheep_port = get_sheep_port(dbg, sr_uuid)

        dog_vdi_create(dbg, sheep_port , '__meta__', '8M')
        length = len(data)
        dog_vdi_write(dbg, sheep_port, '__meta__', pack("!I%ss" % length, length, data), 0, length+4)

    def destroy(self, dbg, uri):
        log.debug("%s: xcpng.libsbd.meta.MetaDBOpeations.destroy: uri: %s" % (dbg, uri))
        dog_vdi_delete(dbg, get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)), '__meta__')

    def load(self, dbg, uri):
        log.debug("%s: xcpng.libsbd.meta.MetaDBOpeations.load: uri: %s" % (dbg, uri))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        sheep_port = get_sheep_port(dbg, sr_uuid)

        length = unpack('!I', dog_vdi_read(dbg, sheep_port, '__meta__', 0, 4))[0]
        data = unpack('!%ss' % length, dog_vdi_read(dbg, sheep_port, '__meta__', 4, length))[0]
        return data

    def dump(self, dbg, uri, json):
        log.debug("%s: xcpng.libsbd.meta.MetaDBOpeations.dump: uri: %s" % (dbg, uri))

        length = len(json)
        dog_vdi_write(dbg,
                      get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)),
                      '__meta__',
                      pack("!I%ss" % length, length, json),
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

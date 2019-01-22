#!/usr/bin/env python

from json import dumps, loads
from struct import pack, unpack

from xapi.storage import log

from tinydb import TinyDB, Query, where
from tinydb.operations import delete
from tinydb.storages import MemoryStorage

from xapi.storage.libs.xcpng.meta import *
from xapi.storage.libs.xcpng.utils import get_sr_uuid_by_uri, get_vdi_uuid_by_uri
from xapi.storage.libs.xcpng.libsbd.sbd_utils import dog_vdi_create, dog_vdi_write, dog_vdi_read, dog_vdi_delete, \
                                                     get_sheep_port

class SBDMetadataHandler(MetadataHandler):

    @staticmethod
    def _create(dbg, uri):
        log.debug("%s: meta.SBDMetadataHandler._create: uri: %s"
                  % (dbg, uri))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        sheep_port = get_sheep_port(dbg, sr_uuid)

        db = TinyDB(storage=MemoryStorage, default_table='pool')
        dog_vdi_create(dbg, sheep_port , '__meta__', '8M')
        data = dumps(db._storage.read())
        length = len(data)
        dog_vdi_write(dbg, sheep_port, '__meta__', pack("!I%ss" % length, length, data), 0, length+4)

    def _destroy(dbg, uri):
        log.debug("%s: meta.SBDMetadataHandler._destroy: uri: %s"
                  % (dbg, uri))
        dog_vdi_delete(dbg, get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)), '__meta__')

    @staticmethod
    def _remove(dbg, uri):
        log.debug("%s: meta.SBDMetadataHandler._remove: uri: %s"
                  % (dbg, uri))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        vdi_uuid = get_vdi_uuid_by_uri(dbg, uri)
        sheep_port = get_sheep_port(dbg, sr_uuid)

        try: # ignore exception if metadata doesn't exist. Fix it later
            db = TinyDB(storage=MemoryStorage, default_table='pool')
            length = unpack('!I', dog_vdi_read(dbg, sheep_port, '__meta__', 0, 4))[0]
            data = unpack('!%ss' % length, dog_vdi_read(dbg, sheep_port, '__meta__', 4, length))[0]
            db._storage.write(loads(data))

            if vdi_uuid != '':
                table = db.table('vdis')
                uuid_tag = UUID_TAG
                uuid = vdi_uuid
            else:
                table = db.table('pool')
                uuid_tag = SR_UUID_TAG
                uuid = sr_uuid

            try:
                table.remove(where(uuid_tag) == uuid)
            except Exception:
                raise Exception("Failed to remove metadata for uri %s" % uri)

            data = dumps(db._storage.read())
            length = len(data)
            dog_vdi_write(dbg, sheep_port, '__meta__', pack("!I%ss" % length, length, data), 0, length + 4)
        except:
            pass

    @staticmethod
    def _load(dbg, uri):
        log.debug("%s: meta.SBDMetadataHandler._load: uri: %s"
                  % (dbg, uri))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        vdi_uuid = get_vdi_uuid_by_uri(dbg, uri)
        sheep_port = get_sheep_port(dbg, sr_uuid)

        #try: # ignore exception if metadata doesn't exist. Fix it later
        db = TinyDB(storage=MemoryStorage, default_table='pool')
        length = unpack('!I', dog_vdi_read(dbg, sheep_port, '__meta__', 0, 4))[0]
        data = unpack('!%ss' % length, dog_vdi_read(dbg, sheep_port, '__meta__', 4, length))[0]
        db._storage.write(loads(data))

        if vdi_uuid != '':
            table = db.table('vdis')
            uuid_tag = UUID_TAG
            uuid = vdi_uuid
        else:
            table = db.table('pool')
            uuid_tag = SR_UUID_TAG
            uuid = sr_uuid

        #try:
        if uuid_tag == SR_UUID_TAG and uuid == '12345678-1234-1234-1234-123456789012':
            image_meta = table.all()[0]
        else:
            image_meta = table.search(where(uuid_tag) == uuid)[0]

        log.debug("%s: meta.SBDMetadataHandler._load: Pool/Image_name: %s Metadata: %s "
                    % (dbg, uuid, image_meta))
        #except Exception:
        #    raise Exception("Failed to load metadata for uri %s" % uri)

        return image_meta
        #except:
        #    return {}

    @staticmethod
    def _update(dbg, uri, image_meta):
        log.debug("%s: meta.SBDMetadataHandler._update_meta: uri: %s image_meta: %s"
                  % (dbg, uri, image_meta))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        vdi_uuid = get_vdi_uuid_by_uri(dbg, uri)
        sheep_port = get_sheep_port(dbg, sr_uuid)

        log.debug("%s: meta.SBDMetadataHandler._update_meta: sr_uuid: %s vdi_uuid: '%s'"
                 % (dbg, sr_uuid, vdi_uuid))

        try: # ignore exception if metadata doesn't exist. Fix it later
            db = TinyDB(storage=MemoryStorage, default_table='pool')
            length = unpack('!I', dog_vdi_read(dbg, sheep_port, '__meta__', 0, 4))[0]
            data = unpack('!%ss' % length, dog_vdi_read(dbg, sheep_port, '__meta__', 4, length))[0]
            db._storage.write(loads(data))

            if vdi_uuid != '':
                table = db.table('vdis')
                uuid_tag = UUID_TAG
                uuid = vdi_uuid
            else:
                table = db.table('pool')
                uuid_tag = SR_UUID_TAG
                uuid = sr_uuid

            if table.search(Query()[uuid_tag] == uuid):
                try:
                    for tag, value in image_meta.iteritems():
                        if value is None:
                            log.debug("%s: meta.SBDMetadataHandler._update_meta: tag: %s remove value" % (dbg, tag))
                            table.update(delete(tag), Query()[uuid_tag] == uuid)
                        else:
                            log.debug("%s: meta.SBDMetadataHandler._update_meta: tag: %s set value: %s" % (dbg, tag, value))
                            table.update({tag: value}, Query()[uuid_tag] == uuid)
                except Exception:
                    raise Exception("Failed to update metadata for uri %s" % uri)
            else:
                try:
                    table.insert(image_meta)
                except Exception:
                    raise Exception("Failed to update metadata for uri %s" % uri)

            data = dumps(db._storage.read())
            length = len(data)
            dog_vdi_write(dbg, sheep_port, '__meta__', pack("!I%ss" % length, length, data), 0, length + 4)
        except:
            pass

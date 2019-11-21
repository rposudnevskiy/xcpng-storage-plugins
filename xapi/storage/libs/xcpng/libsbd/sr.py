#!/usr/bin/env python

from os import system
from xapi.storage import log
from xapi.storage.libs.xcpng.sr import SROperations as _SROperations_
from xapi.storage.libs.xcpng.libsbd.sbd_utils import create_chroot, delete_chroot, \
                                                     set_chroot, unset_chroot, \
                                                     start_sheepdog_gateway, stop_sheepdog_gateway, \
                                                     gen_corosync_conf, write_corosync_conf, \
                                                     dog_node_info, dog_vdi_list, \
                                                     get_sheep_port, free_sheep_port, CHROOT_BASE
from xapi.storage.libs.xcpng.utils import SR_PATH_PREFIX, VDI_PREFIXES, call, \
                                          get_sr_uuid_by_uri, mkdir_p, get_vdi_type_by_uri


class SROperations(_SROperations_):

    def __init__(self):
        self.DEFAULT_SR_NAME = '<Sheepdog SR>'
        self.DEFAULT_SR_DESCRIPTION = '<Sheepdog SR>'
        super(SROperations, self).__init__()

    def create(self, dbg, uri, configuration):
        log.debug("%s: xcpng.libsbd.sr.SROperations.create: uri: %s configuration %s" % (dbg, uri, configuration))

        if 'bindnetaddr' not in configuration:
            raise Exception('Failed to connect to Sheepdog cluster. Parameter \'bindnetaddr\' is not specified')
        elif 'mcastaddr' not in configuration:
            raise Exception('Failed to connect to Sheepdog cluster. Parameter \'mcastaddr\' is not specified')
        elif 'mcastport' not in configuration:
            raise Exception('Failed to connect to Sheepdog cluster. Parameter \'mcastport\' is not specified')

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)

        create_chroot(dbg, sr_uuid)
        set_chroot(dbg, sr_uuid)
        write_corosync_conf(dbg,
                            sr_uuid,
                            gen_corosync_conf(dbg,
                                              configuration['bindnetaddr'],
                                              configuration['mcastaddr'],
                                              configuration['mcastport']
                                              )
                            )
        start_sheepdog_gateway(dbg, get_sheep_port(dbg, sr_uuid), sr_uuid)

    def destroy(self, dbg, uri):
        log.debug("%s: xcpng.libsbd.sr.SROperations.destroy: uri: %s" % (dbg, uri))
        self.MetadataHandler.destroy(dbg, uri)

    def get_sr_list(self, dbg, uri, configuration):
        log.debug("%s: xcpng.libsbd.sr.SROperations.get_sr_list: uri: %s configuration %s" % (dbg, uri, configuration))
        return ["%s/12345678-1234-1234-1234-123456789012" % uri]

    def get_vdi_list(self, dbg, uri):
        log.debug("%s: xcpng.libsbd.sr.SROperations.get_vdi_list: uri: %s" % (dbg, uri))
        vols = []
        for vol in dog_vdi_list(dbg, get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri))):
            if vol.startswith(VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)]):
                vols.append(vol)
        return vols

    def sr_import(self, dbg, uri, configuration):
        log.debug("%s: xcpng.libsbd.sr.SROperations.sr_import: uri: %s configuration %s" % (dbg, uri, configuration))

        if 'bindnetaddr' not in configuration:
            raise Exception('Failed to connect to Sheepdog cluster. Parameter \'bindnetaddr\' is not specified')
        elif 'mcastaddr' not in configuration:
            raise Exception('Failed to connect to Sheepdog cluster. Parameter \'mcastaddr\' is not specified')
        elif 'mcastport' not in configuration:
            raise Exception('Failed to connect to Sheepdog cluster. Parameter \'mcastport\' is not specified')

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)

        create_chroot(dbg, sr_uuid)
        set_chroot(dbg, sr_uuid)
        write_corosync_conf(dbg,
                            sr_uuid,
                            gen_corosync_conf(dbg,
                                              configuration['bindnetaddr'],
                                              configuration['mcastaddr'],
                                              configuration['mcastport']
                                              )
                            )

        start_sheepdog_gateway(dbg, get_sheep_port(dbg, sr_uuid), sr_uuid)

        mkdir_p("%s/%s" % (SR_PATH_PREFIX, get_sr_uuid_by_uri(dbg, uri)))

        call(dbg, ['ln',
                   '-s',
                   "%s/%s/var/lib/sheepdog/sock" % (CHROOT_BASE,get_sr_uuid_by_uri(dbg, uri)),
                   "%s/%s/sock" % (SR_PATH_PREFIX, get_sr_uuid_by_uri(dbg, uri))])

    def sr_export(self, dbg, uri):
        log.debug("%s: xcpng.libsbd.sr.SROperations.sr_export: uri: %s" % (dbg, uri))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        stop_sheepdog_gateway(dbg, sr_uuid)
        free_sheep_port(dbg, sr_uuid)
        unset_chroot(dbg, sr_uuid)
        delete_chroot(dbg, sr_uuid)
        system("rm -rf %s/%s" % (SR_PATH_PREFIX, get_sr_uuid_by_uri(dbg, uri)))

    def get_free_space(self, dbg, uri):
        log.debug("%s: xcpng.libsbd.sr.SROperations.get_free_space: uri: %s" % (dbg, uri))
        return int(dog_node_info(dbg, get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)))[3])

    def get_size(self, dbg, uri):
        log.debug("%s: xcpng.libsbd.sr.SROperations.sr_size: uri: %s" % (dbg, uri))
        return int(dog_node_info(dbg, get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)))[1])

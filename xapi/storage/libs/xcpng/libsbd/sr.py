#!/usr/bin/env python

from os import system

from xapi.storage.libs.xcpng.sr import SROperations, SR, Implementation
from xapi.storage.libs.xcpng.libsbd.sbd_utils import create_chroot, delete_chroot, \
                                                     set_chroot, unset_chroot, \
                                                     start_sheepdog_gateway, stop_sheepdog_gateway, \
                                                     gen_corosync_conf, write_corosync_conf, \
                                                     dog_vdi_list, dog_node_info, \
                                                     get_sheep_port, free_sheep_port
from xapi.storage.libs.xcpng.utils import POOL_PREFIX, VDI_PREFIXES, SR_PATH_PREFIX, \
                                          get_sr_uuid_by_uri, get_vdi_type_by_uri
from xapi.storage.libs.xcpng.libsbd.meta import SBDMetadataHandler


class SBDSROperations(SROperations):

    def __init__(self):
        super(SBDSROperations, self).__init__()
        self.DEFAULT_SR_NAME = '<Sheepdog SR>'
        self.DEFAULT_SR_DESCRIPTION = '<Sheepdog SR>'

    def sr_import(self, dbg, uri, configuration):
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

        system("mkdir -p %s/%s" % (SR_PATH_PREFIX, get_sr_uuid_by_uri(dbg, uri)))

    def sr_export(self, dbg, uri):
        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        stop_sheepdog_gateway(dbg, sr_uuid)
        free_sheep_port(dbg, sr_uuid)
        unset_chroot(dbg, sr_uuid)
        delete_chroot(dbg, sr_uuid)
        system("rm -rf %s/%s" % (SR_PATH_PREFIX, get_sr_uuid_by_uri(dbg, uri)))

    def create(self, dbg, uri, configuration):
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
        pass

    def get_sr_list(self, dbg, configuration):
        return ["SBD%s12345678-1234-1234-1234-123456789012" % POOL_PREFIX]

    def get_vdi_list(self, dbg, uri):
        vdis = []
        for vdi in dog_vdi_list(dbg, get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri))):
            if vdi[1].startswith(VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)]):
                vdis.append(vdi[1])
        return vdis

    def get_free_space(self, dbg, uri):
        return int(dog_node_info(dbg, get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)))[3])

    def get_size(self, dbg, uri):
        return int(dog_node_info(dbg, get_sheep_port(dbg, get_sr_uuid_by_uri(dbg, uri)))[1])


class SBDSR(SR):

    def __init__(self):
        super(SBDSR, self).__init__()
        self.sr_type = 'sbd'
        self.MetadataHandler = SBDMetadataHandler
        self.SROpsHendler = SBDSROperations()


class SBDImplementation(Implementation):

    def __init__(self):
        super(SBDImplementation, self).__init__()
        self.SR = SBDSR()
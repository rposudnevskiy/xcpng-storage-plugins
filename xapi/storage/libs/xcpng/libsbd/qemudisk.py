#!/usr/bin/env python

from xapi.storage.libs.xcpng.qemudisk import Qemudisk
from xapi.storage.libs.xcpng.qemudisk import ROOT_NODE_NAME
from xapi.storage.libs.xcpng.utils import VDI_PREFIXES
from xapi.storage.libs.xcpng.libsbd.sbd_utils import get_sheep_port


class SBDQemudisk(Qemudisk):

    def _set_open_args_(self, dbg):

        self.open_args = {'driver': self.vdi_type,
                           'cache': {'direct': True, 'no-flush': True},
                           # 'discard': 'unmap',
                           'file': {'driver': 'sheepdog',
                                    'server': {'type': 'inet',
                                               'host': '127.0.0.1',
                                               'port': "%s" % get_sheep_port(dbg, self.sr_uuid)},
                                    'vdi': "%s%s" % (VDI_PREFIXES[self.vdi_type], self.vdi_uuid)},
                                    # 'node-name': RBD_NODE_NAME},
                           "node-name": ROOT_NODE_NAME}
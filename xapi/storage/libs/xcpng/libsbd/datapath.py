#!/usr/bin/env python

from xapi.storage.libs.xcpng.datapath import QdiskDatapath, Implementation
from xapi.storage.libs.xcpng.libsbd.qemudisk import SBDQemudisk
from xapi.storage.libs.xcpng.libsbd.meta import SBDMetadataHandler


class SBDQdiskDatapath(QdiskDatapath):

    def __init__(self):
        super(SBDQdiskDatapath, self).__init__()
        self.MetadataHandler = SBDMetadataHandler
        self.qemudisk = SBDQemudisk


class SBDImplementation(Implementation):

    def __init__(self):
        super(SBDImplementation, self).__init__()
        self.Datapath = SBDQdiskDatapath()

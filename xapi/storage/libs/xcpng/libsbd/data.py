#!/usr/bin/env python

from xapi.storage.libs.xcpng.data import QdiskData, Implementation
from xapi.storage.libs.xcpng.libsbd.qemudisk import SBDQemudisk
from xapi.storage.libs.xcpng.libsbd.meta import SBDMetadataHandler


class SBDQdiskData(QdiskData):

    def __init__(self):
        super(SBDQdiskData, self).__init__()
        self.MetadataHandler = SBDMetadataHandler
        self.qemudisk = SBDQemudisk


class ZFSImplementation(Implementation):

    def __init__(self):
        super(ZFSImplementation, self).__init__()
        self.Datapath = SBDQdiskData()

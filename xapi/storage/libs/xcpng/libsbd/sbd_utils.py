#!/usr/bin/env python

import re
import pickle

from subprocess import Popen, PIPE
from os import system, path
from time import sleep

from xapi.storage import log
from xapi.storage.libs.xcpng.utils import call

VOLBLOCKSIZE=4194304
CHROOT_BASE = '/var/tmp/SBDSR/chroot'
START_SHEEP_PORT = 7000
SHEEP_PORTS_DB = '/var/tmp/SBDSR/sheep_ports'


def create_chroot(dbg, name):
    log.debug("%s: sbd_utils.create_chroot: name: %s " % (dbg, name))
    system("mkdir -p %s/%s/dev" % (CHROOT_BASE, name))
    system("mkdir -p %s/%s/proc" % (CHROOT_BASE, name))
    system("mkdir -p %s/%s/sys" % (CHROOT_BASE, name))
    system("mkdir -p %s/%s/tmp" % (CHROOT_BASE, name))
    system("mkdir -p %s/%s/etc/corosync" % (CHROOT_BASE, name))
    system("mkdir -p %s/%s/usr/lib64" % (CHROOT_BASE, name))
    system("mkdir -p %s/%s/usr/sbin" % (CHROOT_BASE, name))
    system("mkdir -p %s/%s/usr/bin" % (CHROOT_BASE, name))
    system("mkdir -p %s/%s/run" % (CHROOT_BASE, name))
    system("mkdir -p %s/%s/var/log/cluster" % (CHROOT_BASE, name))
    system("mkdir -p %s/%s/var/lib/corosync" % (CHROOT_BASE, name))
    system("mkdir -p %s/%s/var/lib/sheepdog" % (CHROOT_BASE, name))
    system("cp /usr/sbin/corosync %s/%s/usr/sbin" % (CHROOT_BASE, name))
    system("cp /usr/sbin/sheep %s/%s/usr/sbin" % (CHROOT_BASE, name))
    system("cp /usr/bin/pkill %s/%s/usr/bin" % (CHROOT_BASE, name))
    system("ln -s ../run %s/%s/var/run" % (CHROOT_BASE, name))
    system("ln -s usr/lib64 %s/%s/lib64" % (CHROOT_BASE, name))

def delete_chroot(dbg, name):
    log.debug("%s: sbd_utils.delete_chroot: name: %s " % (dbg, name))
    system("rm -rf %s/%s" % (CHROOT_BASE, name))

def set_chroot(dbg, name):
    log.debug("%s: sbd_utils.set_chroot: name: %s " % (dbg, name))
    system("mount --rbind /dev %s/%s/dev" % (CHROOT_BASE, name))
    system("mount --make-rslave %s/%s/dev" % (CHROOT_BASE, name))
    system("mount --rbind /sys %s/%s/sys" % (CHROOT_BASE, name))
    system("mount --make-rslave %s/%s/sys" % (CHROOT_BASE, name))
    system("mount --rbind /usr/lib64 %s/%s/usr/lib64" % (CHROOT_BASE, name))
    system("mount -o remount,ro,rbind %s/%s/usr/lib64" % (CHROOT_BASE, name))
    system("mount --make-rslave %s/%s/usr/lib64" % (CHROOT_BASE, name))
    system("mount --rbind /tmp %s/%s/tmp" % (CHROOT_BASE, name))
    system("mount -t proc /proc %s/%s/proc" % (CHROOT_BASE, name))

def unset_chroot(dbg, name):
    log.debug("%s: sbd_utils.unset_chroot: name: %s " % (dbg, name))
    system("mount | grep %s/%s | awk '{print $3}' | sort -rn | xargs -l1 umount -f" % (CHROOT_BASE, name))
    sleep(5)

def start_sheepdog_gateway(dbg, port, name):
    log.debug("%s: sbd_utils.start_sheepdog_gateway: port: %s name: %s " % (dbg, port, name))
    system("chroot %s/%s /usr/sbin/corosync" % (CHROOT_BASE, name))
    system("chroot %s/%s /usr/sbin/sheep --bindaddr 0.0.0.0 --port %s --cluster corosync --log dir=/var/log,level=debug --gateway" %
           (CHROOT_BASE, name, port))
    sleep(5)

def stop_sheepdog_gateway(dbg, name):
    log.debug("%s: sbd_utils.stop_sheepdog_gateway: name: %s " % (dbg, name))
    system("chroot %s/%s /usr/bin/pkill sheep" % (CHROOT_BASE, name))
    system("chroot %s/%s /usr/bin/pkill corosync" % (CHROOT_BASE, name))

def gen_corosync_conf(dbg, bindnetaddr, mcastaddr, mcastport):
    log.debug("%s: sbd_utils.gen_corosync_conf: bindnetaddr %s, mcastaddr %s, mcastport %s" %
              (dbg, bindnetaddr, mcastaddr, mcastport))
    return "compatibility: whitetank\n\
totem {\n\
\tversion: 2\n\
\tsecauth: off\n\
\tthreads: 0\n\
\tinterface {\n\
\t\tringnumber: 0\n\
\t\tbindnetaddr: %s\n\
\t\tmcastaddr: %s\n\
\t\tmcastport: %s\n\
\t}\n\
}\n\
logging {\n\
\tfileline: off\n\
\tto_stderr: no\n\
\tto_logfile: yes\n\
\tto_syslog: yes\n\
\tlogfile: /var/log/cluster/corosync.log\n\
\tdebug: off\n\
\ttimestamp: on\n\
\tlogger_subsys {\n\
\t\tsubsys: AMF\n\
\t\tdebug: off\n\
\t}\n\
}\n\
amf {\n\
\tmode: disabled\n\
}" % (bindnetaddr, mcastaddr, mcastport)

def get_sheep_port(dbg, name):
    log.debug("%s: sbd_utils.get_sheep_port: name: %s " % (dbg, name))
    SRS_MAX = 32
    port_found = False
    if not path.isfile(SHEEP_PORTS_DB):
        ports = [None] * SRS_MAX
        port = 0
    else:
        with open(SHEEP_PORTS_DB, 'rb') as fd:
            ports = pickle.load(fd)
            fd.close()

        port = None

        for _index_, _name_  in enumerate(ports):
            if _name_ is not None:
                if _name_ == name:
                    port = _index_
                    port_found = True
                    break

        if port is None:
            for _index_, _name_ in enumerate(ports):
                if _name_ is None:
                    port = _index_
                    ports[port] = name
                    break

        if port is None:
            raise Exception('Failed to get/allocate port for sheep daemon')

    if not port_found:
        with open(SHEEP_PORTS_DB, 'wb') as fd:
            pickle.dump(ports, fd)
            fd.close()

    return START_SHEEP_PORT+port

def free_sheep_port(dbg, name):
    log.debug("%s: sbd_utils.free_sheep_port: name: %s " % (dbg, name))
    with open(SHEEP_PORTS_DB, 'rb') as fd:
        ports = pickle.load(fd)

    for _index_, _name_ in enumerate(ports):
        if _name_ == name:
            ports[_index_] = None
            break

    with open(SHEEP_PORTS_DB, 'wb') as fd:
        pickle.dump(ports, fd)
        fd.close()

def write_corosync_conf(dbg, name, config):
    log.debug("%s: sbd_utils.free_sheep_port: name: %s " % (dbg, name))
    with open("%s/%s/etc/corosync/corosync.conf" % (CHROOT_BASE, name), "w") as fd:
        fd.write(config)
        fd.close()

def dog_vdi_write(dbg, port, vdi_name, buffer='', offset=None, length=None):
    log.debug("%s: sbd_utils.dog_vdi_write: port: %s vdi_name: %s buffer: %s offset: %s length: %s" %
              (dbg, port, vdi_name, buffer[4:], offset, length))
    cmd = ['dog', 'vdi', 'write', '-p', str(port), vdi_name]
    if offset is not None:
        cmd.extend([str(offset)])
        if length is not None:
            cmd.extend([str(length)])

    proc = Popen(cmd,stdin=PIPE)
    proc.stdin.write(buffer)
    proc.wait()

def dog_vdi_read(dbg, port, vdi_name, offset=None, length=None):
    log.debug("%s: sbd_utils.dog_vdi_read: port: %s vdi_name: %s offset: %s length: %s" %
              (dbg, port, vdi_name, offset, length))
    cmd = ['dog', 'vdi', 'read', '-p', str(port), vdi_name]
    if offset is not None:
        cmd.extend([str(offset)])
        if length is not None:
            cmd.extend([str(length)])
    proc = Popen(cmd,stdout=PIPE)
    data = proc.stdout.read(length)
    proc.wait()
    return data

def dog_vdi_create(dbg, port, vdi_name, size):
    log.debug("%s: sbd_utils.dog_vdi_create: port: %s vdi_name: %s size: %s" % (dbg, port, vdi_name, size))
    call(dbg, ['dog', 'vdi', 'create', '-p', str(port), vdi_name, str(size)])


def dog_vdi_delete(dbg, port, vdi_name):
    log.debug("%s: sbd_utils.dog_vdi_delete: port: %s vdi_name: %s" % (dbg, port, vdi_name))
    call(dbg, ['dog', 'vdi', 'delete', '-p', str(port), vdi_name])

def dog_vdi_resize(dbg, port, vdi_name, new_size):
    log.debug("%s: sbd_utils.dog_vdi_resize: port: %s vdi_name: %s new_size: %s " % (dbg, port, vdi_name, new_size))
    call(dbg, ['dog', 'vdi', 'resize', '-p', str(port), vdi_name, new_size])

def dog_vdi_list(dbg, port):
    log.debug("%s: sbd_utils.dog_vdi_list: port: %s" % (dbg, port))
    vdis = []
    proc = Popen(['dog', 'vdi', 'list', '-p', str(port), '-r'], stdout=PIPE)
    for line in iter(proc.stdout.readline, ''):
        vdis.append(line.split(' ')[1])
    return vdis

def dog_vdi_info(dbg, port, vdi):
    log.debug("%s: sbd_utils.dog_vdi_info: port: %s vdi: %s" % (dbg, port, vdi))
    proc = Popen(['dog', 'vdi', 'list', '-p', str(port), '-r', vdi], stdout=PIPE)
    for line in iter(proc.stdout.readline, ''):
        return line.split(' ')

def dog_node_info(dbg, port, node_id='Total'):
    log.debug("%s: sbd_utils.dog_node_info: port: %s node_id: %s" % (dbg, port, node_id))
    proc = Popen(['dog', 'node', 'info', '-p', str(port), '-r'], stdout=PIPE)
    regex = re.compile("^%s" % node_id)
    for line in iter(proc.stdout.readline, ''):
        result = regex.match(line)
        if result:
            return line.split(' ')

def dog_vdi_setattr(dbg, port, vdi_name, attr, val, exclusive=True):
    log.debug("%s: xcpng.libsbd.sbd_utils.dog_vdi_setattr: port: %s vdi: %s attr: %s val: %s excl: %s" %
              (dbg, port, vdi_name, attr, val, exclusive))
    cmd = ['dog', 'vdi', 'setattr', '-p', str(port)]
    if exclusive:
        cmd.append('-x')
    cmd.extend([vdi_name, attr, val])
    call(dbg, cmd)

def dog_vdi_delattr(dbg, port, vdi_name, attr):
    log.debug("%s: xcpng.libsbd.sbd_utils.dog_vdi_delattr: port: %s vdi: %s attr: %s" %
              (dbg, port, vdi_name, attr))
    call(dbg, ['dog', 'vdi', 'setattr', '-p', str(port), '-d', vdi_name, attr])

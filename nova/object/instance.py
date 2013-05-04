#    Copyright 2013 IBM Corp.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import datetime
import netaddr

# TEMPORARY!
from nova import db
from nova.object import base
from nova.openstack.common import timeutils
from nova import utils

def datetime_or_none(dt):
    if dt is None or isinstance(dt, datetime.datetime):
        return dt
    raise ValueError('A datetime.datetime is required here')

def int_or_none(val):
    if val is None:
        return val
    else:
        return int(val)

def ip_or_none(version):

    def validator(val, version=version):
        if val is None:
            return val
        else:
            return netaddr.IPAddress(val, version=version)

    return validator

class Instance(base.NovaObject):
    fields = {
        'id': int,

        'user_id': str,
        'project_id': str,

        'image_ref': str,
        'kernel_id': str,
        'ramdisk_id': str,
        'hostname': str,

        'launch_index': int,
        'key_name': str,
        'key_data': str,

        'power_state': int_or_none,
        'vm_state': str,
        'task_state': str,

        'memory_mb': int,
        'vcpus': int,
        'root_gb': int,
        'ephemeral_gb': int,

        'host': str,
        'node': str,

        'instance_type_id': int,

        'user_data': str,

        'reservation_id': str,

        'scheduled_at': datetime_or_none,
        'launched_at': datetime_or_none,
        'terminated_at': datetime_or_none,

        'availability_zone': str,

        'display_name': str,
        'display_description': str,

        'launched_on': str,
        'locked': bool,

        'os_type': str,
        'architecture': str,
        'vm_mode': str,
        'uuid': str,

        'root_device_name': str,
        'default_ephemeral_device': str,
        'default_swap_device': str,
        'config_drive': str,

        'access_ip_v4': ip_or_none(4),
        'access_ip_v6': ip_or_none(6),

        'auto_disk_config': bool,
        'progress': int,

        'shutdown_terminate': bool,
        'disable_terminate': bool,

        'cell_name': str,

        'metadata': dict,
        'system_metadata': dict,

        }

    def _attr_access_ip_v4_to_primitive(self):
        return str(self.access_ip_v4)

    def _attr_access_ip_v6_to_primitive(self):
        return str(self.access_ip_v6)

    def _attr_scheduled_at_to_primitive(self):
        return timeutils.isotime(self.scheduled_at)

    def _attr_launched_at_to_primitive(self):
        return timeutils.isotime(self.launched_at)

    def _attr_terminated_at_to_primitive(self):
        return timeutils.isotime(self.terminated_at)

    def _attr_scheduled_at_from_primitive(self, value):
        return timeutils.parse_isotime(value)

    def _attr_launched_at_from_primitive(self, value):
        print "PARSE"
        return timeutils.parse_isotime(value)

    def _attr_terminated_at_from_primitive(self, value):
        return timeutils.parse_isotime(value)

    @base.magic_static
    def get(cls, context, instance_uuid=None, expected_attrs=None):
        if expected_attrs is None:
            expected_attrs = []

        # Construct DB-specific columns from generic expected_attrs
        columns_to_join = []
        if 'metadata' in expected_attrs:
            columns_to_join.append('metadata')
        if 'system_metadata' in expected_attrs:
            columns_to_join.append('system_metadata')

        db_inst = db.instance_get_by_uuid(context, instance_uuid,
                                          columns_to_join)

        instance = cls()
        # Most of the field names match right now, so be quick
        for field in cls.fields:
            if field in ['metadata', 'system_metadata']:
                continue
            instance[field] = db_inst[field]

        if 'metadata' in expected_attrs:
            instance['metadata'] = utils.metadata_to_dict(db_inst['metadata'])
        if 'system_metadata' in expected_attrs:
            instance['system_metadata'] = utils.metadata_to_dict(
                db_inst['system_metadata'])

        instance.reset_changes()
        return instance

    @base.magic
    def save(self, context):
        updates = dict()
        for field in self.what_changed():
            updates[field] = self[field]
        db.instance_update(context, self.uuid, updates)
        self.reset_changes()

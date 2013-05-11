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
from nova import notifications
from nova.object import base
from nova.openstack.common import timeutils
from nova import utils

from oslo.config import cfg

CONF = cfg.CONF


def datetime_or_none(dt):
    if dt is None or isinstance(dt, datetime.datetime):
        return dt
    raise ValueError('A datetime.datetime is required here')

def int_or_none(val):
    if val is None:
        return val
    else:
        return int(val)

def str_or_none(val):
    if val is None:
        return val
    else:
        return str(val)

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

        'launch_index': int_or_none,
        'key_name': str,
        'key_data': str,

        'power_state': int_or_none,
        'vm_state': str_or_none,
        'task_state': str_or_none,

        'memory_mb': int_or_none,
        'vcpus': int_or_none,
        'root_gb': int_or_none,
        'ephemeral_gb': int_or_none,

        'host': str,
        'node': str,

        'instance_type_id': int_or_none,

        'user_data': str,

        'reservation_id': str,

        'created_at': datetime_or_none,
        'updated_at': datetime_or_none,
        'deleted_at': datetime_or_none,
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
        'progress': int_or_none,

        'shutdown_terminate': bool,
        'disable_terminate': bool,

        'cell_name': str,

        'metadata': dict,
        'system_metadata': dict,

        }

    @property
    def name(self):
        try:
            base_name = CONF.instance_name_template % self.id
        except TypeError:
            # Support templates like "uuid-%(uuid)s", etc.
            info = {}
            # NOTE(russellb): Don't use self.iteritems() here, as it will
            # result in infinite recursion on the name property.
            for key in self.fields:
                # prevent recursion if someone specifies %(name)s
                # %(name)s will not be valid.
                if key == 'name':
                    continue
                info[key] = self[key]
            try:
                base_name = CONF.instance_name_template % info
            except KeyError:
                base_name = self.uuid
        return base_name

    def _attr_access_ip_v4_to_primitive(self):
        if self.access_ip_v4 is not None:
            return str(self.access_ip_v4)
        else:
            return None

    def _attr_access_ip_v6_to_primitive(self):
        if self.access_ip_v6 is not None:
            return str(self.access_ip_v6)
        else:
            return None

    def _attr_created_at_to_primitive(self):
        return timeutils.isotime(self.created_at)

    def _attr_updated_at_to_primitive(self):
        return timeutils.isotime(self.updated_at)

    def _attr_deleted_at_to_primitive(self):
        return timeutils.isotime(self.deleted_at)

    def _attr_scheduled_at_to_primitive(self):
        return timeutils.isotime(self.scheduled_at)

    def _attr_launched_at_to_primitive(self):
        return timeutils.isotime(self.launched_at)

    def _attr_terminated_at_to_primitive(self):
        return timeutils.isotime(self.terminated_at)

    def _attr_scheduled_at_from_primitive(self, value):
        return timeutils.parse_isotime(value)

    def _attr_created_at_from_primitive(self, value):
        return timeutils.parse_isotime(value)

    def _attr_updated_at_from_primitive(self, value):
        return timeutils.parse_isotime(value)

    def _attr_deleted_at_from_primitive(self, value):
        return timeutils.parse_isotime(value)

    def _attr_launched_at_from_primitive(self, value):
        return timeutils.parse_isotime(value)

    def _attr_terminated_at_from_primitive(self, value):
        return timeutils.parse_isotime(value)

    @classmethod
    def _from_db_object(cls, db_inst, expected_attrs=None):
        """Method to help with migration to objects.

        Converts a database entity to a formal object.
        """
        if expected_attrs is None:
            expected_attrs = []
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

    @base.magic_static
    def get_by_uuid(cls, context, uuid=None, expected_attrs=None):
        if expected_attrs is None:
            expected_attrs = []

        # Construct DB-specific columns from generic expected_attrs
        columns_to_join = []
        if 'metadata' in expected_attrs:
            columns_to_join.append('metadata')
        if 'system_metadata' in expected_attrs:
            columns_to_join.append('system_metadata')

        db_inst = db.instance_get_by_uuid(context, uuid,
                                          columns_to_join)
        return cls._from_db_object(db_inst, expected_attrs)

    @base.magic
    def save(self, context):
        updates = dict()
        changes = self.what_changed()
        for field in changes:
            updates[field] = self[field]
        old_ref, inst_ref = db.instance_update_and_get_original(context,
                                                                self.uuid,
                                                                updates)

        if 'vm_state' in changes or 'task_state' in changes:
            notifications.send_update(context, old_ref, inst_ref)

        self.reset_changes()

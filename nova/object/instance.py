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

def dt_serializer(name):
    def serializer(self, name=name):
        if self[name] is not None:
            return timeutils.isotime(self[name])
        else:
            return None
    return serializer

def dt_deserializer(instance, val):
    if val is None:
        return None
    else:
        return timeutils.parse_isotime(val)

class Instance(base.NovaObject):
    fields = {
        'id': int,

        'user_id': str_or_none,
        'project_id': str_or_none,

        'image_ref': str_or_none,
        'kernel_id': str_or_none,
        'ramdisk_id': str_or_none,
        'hostname': str_or_none,

        'launch_index': int_or_none,
        'key_name': str_or_none,
        'key_data': str_or_none,

        'power_state': int_or_none,
        'vm_state': str_or_none,
        'task_state': str_or_none,

        'memory_mb': int_or_none,
        'vcpus': int_or_none,
        'root_gb': int_or_none,
        'ephemeral_gb': int_or_none,

        'host': str_or_none,
        'node': str_or_none,

        'instance_type_id': int_or_none,

        'user_data': str_or_none,

        'reservation_id': str_or_none,

        'created_at': datetime_or_none,
        'updated_at': datetime_or_none,
        'deleted_at': datetime_or_none,
        'scheduled_at': datetime_or_none,
        'launched_at': datetime_or_none,
        'terminated_at': datetime_or_none,

        'availability_zone': str_or_none,

        'display_name': str_or_none,
        'display_description': str_or_none,

        'launched_on': str_or_none,
        'locked': bool,

        'os_type': str_or_none,
        'architecture': str_or_none,
        'vm_mode': str_or_none,
        'uuid': str_or_none,

        'root_device_name': str_or_none,
        'default_ephemeral_device': str_or_none,
        'default_swap_device': str_or_none,
        'config_drive': str_or_none,

        'access_ip_v4': ip_or_none(4),
        'access_ip_v6': ip_or_none(6),

        'auto_disk_config': bool,
        'progress': int_or_none,

        'shutdown_terminate': bool,
        'disable_terminate': bool,

        'cell_name': str_or_none,

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

    _attr_created_at_to_primitive = dt_serializer('created_at')
    _attr_updated_at_to_primitive = dt_serializer('updated_at')
    _attr_deleted_at_to_primitive = dt_serializer('deleted_at')
    _attr_scheduled_at_to_primitive = dt_serializer('scheduled_at')
    _attr_launched_at_to_primitive = dt_serializer('launched_at')
    _attr_terminated_at_to_primitive = dt_serializer('terminated_at')

    _attr_created_at_from_primitive = dt_deserializer
    _attr_updated_at_from_primitive = dt_deserializer
    _attr_deleted_at_from_primitive = dt_deserializer
    _attr_scheduled_at_from_primitive = dt_deserializer
    _attr_launched_at_from_primitive = dt_deserializer
    _attr_terminated_at_from_primitive = dt_deserializer

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
    def save(self, context, expected_task_state=None):
        """Save updates to this instance

        Column-wise updates will be made based on the result of
        self.what_changed(). If expected_task_state is provided,
        it will be checked against the in-database copy of the
        instance before updates are made.
        :param context: Security context
        :param expected_task_state: Optional tuple of valid task states
                                    for the instance to be in.
        """
        updates = dict()
        changes = self.what_changed()
        for field in changes:
            updates[field] = self[field]
        if expected_task_state is not None:
            updates['expected_task_state'] = expected_task_state
        old_ref, inst_ref = db.instance_update_and_get_original(context,
                                                                self.uuid,
                                                                updates)

        if 'vm_state' in changes or 'task_state' in changes:
            notifications.send_update(context, old_ref, inst_ref)

        self.reset_changes()

    @base.magic
    def refresh(self, context):
        extra = []
        for field in ['system_metadata', 'metadata']:
            if hasattr(self, base.get_attrname(field)):
                extra.append(field)
        current = Instance.get_by_uuid(context, uuid=self.uuid,
                                       expected_attrs=extra)
        for field in self.fields:
            if (hasattr(self, base.get_attrname(field)) and
                self[field] != current[field]):
                self[field] = current[field]

    def load(self, attrname):
        extra = []
        if attrname == 'system_metadata':
            extra.append('system_metadata')
        elif attrname == 'metadata':
            extra.append('metadata')

        if not extra:
            raise Exception('Cannot load "%s" from instance' % attrname)

        # NOTE(danms): This could be optimized to just load the bits we need
        instance = Instance.get_by_uuid(self._context,
                                        uuid=self.uuid,
                                        expected_attrs=extra)
        self[attrname] = instance[attrname]

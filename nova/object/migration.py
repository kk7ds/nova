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

# TEMPORARY!
from nova import db
from nova.object import base

class Migration(base.NovaObject):
    fields = {
        'id': int,
        'source_compute': str,
        'dest_compute': str,
        'source_node': str,
        'dest_node': str,
        'dest_host': str,
        'old_instance_type_id': int,
        'new_instance_type_id': int,
        'instance_uuid': str,
        'status': str}

    # Note: this is silly for migration, but just as a demo...
    def load(self, attrname):
        # Figure out where/how to get context!
        migration = self.get(self._context, self.id)
        for key in self.fields:
            if not hasattr(self, base.get_attrname(key)):
                setattr(self, key, migration[key])

    @base.magic
    def save(self, context):
        # Pretend we have a real implementation here and just make it
        # work with the existing db api for now
        updates = dict()
        for field in self.what_changed():
            updates[field] = self[field]
        db.migration_update(context, self.id, updates)

    @base.magic_static
    def get(cls, context, migration_id):
        migration = cls()
        # Pretend the actual DB implementation is here!
        db_migration = db.migration_get(context, migration_id)
        # Naively construct a migration object
        for attr in cls.fields:
            setattr(migration, attr, db_migration[attr])
        migration.reset_changes()
        return migration

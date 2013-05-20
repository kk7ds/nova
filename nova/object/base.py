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

"""Nova common internal object model"""

import collections

from nova import exception
from nova.openstack.common import log as logging
import nova.openstack.common.rpc.proxy
import nova.openstack.common.rpc.dispatcher
import nova.openstack.common.rpc.serializer


LOG = logging.getLogger('object')

def get_attrname(name):
    """Return the mangled name of the attribute's underlying storage"""
    return '_%s' % name


def make_class_properties(cls):
    """Take cls.fields and create real properties for them"""
    for name, typefn in cls.fields.iteritems():

        def getter(self, name=name, typefn=typefn):
            attrname = get_attrname(name)
            if not hasattr(self, attrname):
                self.load(name)
            return getattr(self, attrname)

        def setter(self, value, name=name, typefn=typefn):
            self._changed_fields.add(name)
            try:
                return setattr(self, get_attrname(name), typefn(value))
            except Exception, e:
                # This should probably be a log call, but print for now
                attr = "%s.%s" % (self.objname(), name)
                LOG.exception(_('Error setting %(attr)s') % locals())
                raise

        setattr(cls, name, property(getter, setter))


class NovaObjectMetaclass(type):
    """Metaclass that allows tracking of object classes."""

    # NOTE(danms): This is what controls whether object operations are
    # remoted. If this is not None, use it to remote things over RPC.
    indirection_api = None

    def __init__(cls, names, bases, dict_):
        if not hasattr(cls, '_obj_classes'):
            # This will be set in the 'NovaObject' class.
            cls._obj_classes = collections.defaultdict(list)
        else:
            # Add the subclass to NovaObject._obj_classes
            make_class_properties(cls)
            cls._obj_classes[cls.objname()].append(cls)


# These are evil decorator things that return either a
# straight-through wrapper to the actual implementation, or a thing
# that actually calls over RPC for this.  The result of the first is a
# classmethod either way, and provides us a way to call
# Object.method() and get either the direct-to-database
# implementation, or RPC indirection to it on some other service
def magic_static(fn):
    def wrapper(cls, context, **kwargs):
        if NovaObject.indirection_api:
            result = NovaObject.indirection_api.object_class_action(
                context, cls.objname(), fn.__name__, cls.version, kwargs)
        else:
            result = fn(cls, context, **kwargs)
            if isinstance(result, NovaObject):
                result._context = context
        return result
    return classmethod(wrapper)

def magic(fn):
    def wrapper(self, context=None, **kwargs):
        if context is None:
            context = self._context
        if context is None:
            raise OrphanedObjectError(method=fn.__name__,
                                      objtype=self.objname())
        if NovaObject.indirection_api:
            updates, result = NovaObject.indirection_api.object_action(
                context, self, fn.__name__, self.version, kwargs)
            for key, value in updates.iteritems():
                self[key] = value
            self.reset_changes(updates.keys())
            return result
        else:
            return fn(self, context, **kwargs)
    return wrapper


class UnsupportedObjectError(exception.NovaException):
    message = _('Unsupported object type %(objtype)s')


class OrphanedObjectError(exception.NovaException):
    message = _('Cannot call %(method)s on orphaned %(objtype)s object')


class IncompatibleObjectVersion(exception.NovaException):
    message = _('Version %(objver)s of %(objname)s is not supported')


class IncompatibleObjectMajorVersion(IncompatibleObjectVersion):
    message = _('Incompatible major version (%(client)s != %(server)s)')


class IncompatibleObjectMinorVersion(IncompatibleObjectVersion):
    message = _('Incompatible minor version (%(client)s > %(server)s)')


# Object versioning rules
#
# Each service has its set of objects, each with a version attached. When
# a client attempts to call an object method, the server checks to see if
# the version of that object matches (in a compatible way) its object
# implementation. If so, cool, and if not, fail.
#
# FIXME: The server could provide compatibility with older major versions of
#        the object, but punt on that for now.
# FIXME: The RpcDispatcher should check the version of incoming objects that
#        are provided as arguments and make sure that they're compabible with
#        the local version of the object.
def check_object_version(server, client):
    try:
        client_major, _client_minor = client.split('.')
        server_major, _server_minor = server.split('.')
        client_minor = int(_client_minor)
        server_minor = int(_server_minor)
    except Exception:
        raise Exception('Invalid version string')

    if client_major != server_major:
        raise IncompatibleObjectMajorVersion(dict(client=client_major,
                                                  server=server_major))
    if client_minor > server_minor:
        raise IncompatibleObjectMinorVersion(dict(client=client_minor,
                                                  server=server_minor))


class NovaObject(object):
    __metaclass__ = NovaObjectMetaclass
    version = '1.0'
    fields = {}

    def __init__(self):
        self._changed_fields = set()
        self._context = None

    @classmethod
    def objname(cls):
        """Return a canonical name for this object which will be used over
        the wire for remote hydration."""
        return cls.__name__

    @classmethod
    def class_from_name(cls, objname, objver):
        if objname not in cls._obj_classes:
            LOG.error(_('Unable to instantiate unregistered object type '
                        '%(objtype)s') % dict(objtype=objname))
            raise UnsupportedObjectError(objtype=objname)

        compatible_match = None
        for objclass in cls._obj_classes[objname]:
            if objclass.version == objver:
                return objclass
            try:
                check_object_version(objclass.version, objver)
                compatible_match = objclass
            except IncompatibleObjectVersion:
                pass

        if compatible_match:
            return compatible_match

        raise IncompatibleObjectVersion(objname=objname,
                                        objver=objver)

    def _attr_from_primitive(self, attribute, value):
        handler = '_attr_%s_from_primitive' % attribute
        if hasattr(self, handler):
            return getattr(self, handler)(value)
        return value

    @classmethod
    def from_primitive(cls, primitive):
        """Simple base-case hydration"""
        objname = primitive['nova_object.name']
        objver = primitive['nova_object.version']
        objdata = primitive['nova_object.data']
        objclass = cls.class_from_name(objname, objver)
        self = objclass()
        for name in self.fields:
            if name in objdata:
                setattr(self, name,
                        self._attr_from_primitive(name, objdata[name]))
        changes = primitive.get('nova_object.changes', [])
        self._changed_fields = set([x for x in changes if x in self.fields])
        return self

    def _attr_to_primitive(self, attribute):
        handler = '_attr_%s_to_primitive' % attribute
        if hasattr(self, handler):
            return getattr(self, handler)()
        else:
            return getattr(self, attribute)

    def to_primitive(self):
        """Simple base-case dehydration"""
        primitive = dict()
        for name in self.fields:
            if hasattr(self, get_attrname(name)):
                primitive[name] = self._attr_to_primitive(name)
        obj = {'nova_object.name': self.objname(),
               'nova_object.version': self.version,
               'nova_object.data': primitive}
        if self.what_changed():
            obj['nova_object.changes'] = list(self.what_changed())
        return obj

    def load(self, attrname):
        """Load an additional attribute from the real object.

        This should use self._conductor, and cache any data that might
        be useful for future load operations.
        """
        raise NotImplementedError("Cannot load '%s' in the base class" % attrname)

    def save(self, context):
        """Save the changed fields back to the store."""
        raise NotImplementedError('Cannot save anything in the base class')

    def what_changed(self):
        return self._changed_fields

    def reset_changes(self, fields=None):
        """Reset the list of fields that have been changed.

        Note that this is NOT "revert to previous values"
        """
        if fields:
            for field in fields:
                self._changed_fields.discard(field)
        else:
            self._changed_fields.clear()

    # dictish syntactic sugar
    def iteritems(self):
        for name in self.fields:
            if hasattr(self, get_attrname(name)):
                yield name, getattr(self, name)

    def __getitem__(self, name):
        return getattr(self, name)

    def __setitem__(self, name, value):
        setattr(self, name, value)

    def get(self, key, value=None):
        return self[key]


class NovaObjectSerializer(nova.openstack.common.rpc.serializer.Serializer):
    """A NovaObject-aware Serializer"""
    def serialize_entity(self, context, entity):
        if (hasattr(entity, 'to_primitive') and
            callable(entity.to_primitive)):
            entity = entity.to_primitive()
        return entity

    def deserialize_entity(self, context, entity):
        if isinstance(entity, dict) and 'nova_object.name' in entity:
            entity = NovaObject.from_primitive(entity)
            entity._context = context
        return entity

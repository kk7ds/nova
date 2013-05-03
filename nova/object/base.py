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

import nova.openstack.common.rpc.proxy


def get_attrname(name):
    """Return the mangled name of the attribute's underlying storage"""
    return '_%s' % name


def make_class_properties(cls):
    """Take cls.fields and create real properties for them"""
    for name, typefn in cls.fields.iteritems():

        def getter(self, name=name, typefn=typefn):
            attrname = get_attrname(name)
            if not hasattr(self, attrname):
                self.load(attrname)
            return getattr(self, attrname)

        def setter(self, value, name=name, typefn=typefn):
            self._changed_fields.add(name)
            return setattr(self, get_attrname(name), typefn(value))

        setattr(cls, name, property(getter, setter))


# Pretend this is a config variable
are_things_remote = False

class NovaObjectMetaclass(type):
    """Metaclass that allows tracking of object classes."""

    def __init__(cls, names, bases, dict_):
        if not hasattr(cls, '_obj_classes'):
            # This will be set in the 'NovaObject' class.
            cls._obj_classes = {}
        else:
            # Add the subclass to NovaObject._obj_classes
            make_class_properties(cls)
            cls._obj_classes[cls.objname()] = cls


# These are evil decorator things that return either a
# straight-through wrapper to the actual implementation, or a thing
# that actually calls over RPC for this.  The result of the first is a
# classmethod either way, and provides us a way to call
# Object.method() and get either the direct-to-database
# implementation, or RPC indirection to it on some other service
def magic_static(fn):
    def wrapper(cls, context, **kwargs):
        if are_things_remote:
            # hackity hack hack
            rpc = NovaObjProxy()
            return rpc.object_class_action(context,
                                           cls.objname(), fn.__name__,
                                           kwargs)
        else:
            return fn(cls, context, **kwargs)
    return classmethod(wrapper)

def magic(fn):
    def wrapper(self, context, **kwargs):
        if are_things_remote:
            # Pretend this is a thing
            rpc = NovaObjProxy()
            return rpc.object_action(context,
                                     self, fn.__name__, kwargs)
        else:
            return fn(self, context, **kwargs)
    return wrapper


class NovaObject(object):
    __metaclass__ = NovaObjectMetaclass
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
    def class_from_name(cls, objname):
        return cls._obj_classes[objname]

    @classmethod
    def from_primitive(cls, primitive):
        """Simple base-case hydration"""
        objname = primitive['nova_object.name']
        objclass = cls._obj_classes[objname]
        self = objclass()
        data = primitive['nova_object.data']
        for name in self.fields:
            if name in data:
                setattr(self, name, data[name])
        changes = primitive.get('nova_object.changes', [])
        self._changed_fields = set([x for x in changes if x in self.fields])
        return self

    def to_primitive(self):
        """Simple base-case dehydration"""
        primitive = dict()
        for name in self.fields:
            if hasattr(self, get_attrname(name)):
                primitive[name] = getattr(self, name)
        obj = {'nova_object.name': self.objname(),
               'nova_object.data': primitive}
        if self.what_changed():
            obj['nova_object.changes'] = self.what_changed()
        return obj

    def load(self, attrname):
        """Load an additional attribute from the real object.

        This should use self._conductor, and cache any data that might
        be useful for future load operations.
        """
        raise NotImplementedError('Cannot load anything in the base class')

    def save(self, context):
        """Save the changed fields back to the store."""
        raise NotImplementedError('Cannot save anything in the base class')

    def what_changed(self):
        return self._changed_fields

    def reset_changes(self):
        """Reset the list of fields that have been changed.

        Note that this is NOT "revert to previous values"
        """
        self._changed_fields.clear()

    # dictish syntactic sugar
    def iteritems(self):
        for name in self.fields:
            yield name, getattr(self, name)

    def __getitem__(self, name):
        return getattr(self, name)

    def __setitem__(self, name, value):
        setattr(self, name, value)


# Hacky thing to use conductor for the moment
# This could, of course, be in conductor's RPCAPI, but I think that
# long-term, we will want conductor to expose another namespace'd API object
# that will provide just these object operations.
class NovaObjProxy(nova.openstack.common.rpc.proxy.RpcProxy):
    """A NovaObject-aware RpcProxy.

    This simply provides encapsulation of the Nova-specific magic object
    serialization and deserialization work. This could be moved back to
    Oslo someday if desired.
    """

    def __init__(self):
        super(NovaObjProxy, self).__init__(topic='conductor',
                                           default_version='1.0')

    @staticmethod
    def make_namespaced_msg(method, namespace, **kwargs):
        s_kwargs = {}
        for key, value in kwargs.iteritems():
            if isinstance(value, NovaObject):
                value = value.to_primitive()
            s_kwargs[key] = value
        return nova.openstack.common.rpc.proxy.RpcProxy.make_namespaced_msg(
            method, namespace, **s_kwargs)

    def call(self, context, msg, topic=None, version=None, timeout=None):
        result = super(NovaObjProxy, self).call(context, msg, topic, version,
                                                timeout)
        if isinstance(result, dict) and 'nova_object.name' in result:
            return NovaObject.from_primitive(result)
        else:
            return result

    def object_class_action(self, context, objname, objmethod, kwargs):
        msg = self.make_msg('object_class_action', objname=objname,
                            objmethod=objmethod, **kwargs)
        return self.call(context, msg)

    def object_action(self, context, objinst, objmethod, kwargs):
        msg = self.make_msg('object_action', objinst=objinst.to_primitive(),
                            objmethod=objmethod, **kwargs)
        return self.call(context, msg)

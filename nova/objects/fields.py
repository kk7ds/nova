import datetime
import functools
import iso8601
import netaddr

from nova.network import model as network_model
from nova.openstack.common.gettextutils import _
from nova.openstack.common import timeutils


def enforce_nullable(fn):
    @functools.wraps(fn)
    def wrapper(self, obj, attr, value):
        if value is None and not self._nullable:
            raise ValueError(_("Field `%s' cannot be None") % attr)
        return fn(self, obj, attr, value)
    return wrapper


class Field(object):
    def __init__(self, nullable=False):
        self._nullable = nullable

    def _coerce(self, value):
        """Attempt to coerce a value to a suitable type."""
        return value

    @enforce_nullable
    def coerce(self, obj, attr, value):
        """Coerce a value to a suitable type.

        This is called any time you set a value on an object, like:

          foo.myint = 1

        and is responsible for making sure that the value (1 here) is of
        the proper type, or can be sanely converted.

        In the base class, this also handles the potentially nullable
        nature of the field and calls a helper of self._coerce(value)
        to do the actual conversion of a real value.

        :param:obj: The object being acted upon
        :param:attr: The name of the attribute/field being set
        :param:value: The value being set
        :returns: The properly-typed value
        """
        if value is None:
            return None
        else:
            return self._coerce(value)

    def _from_primitive(self, value):
        """Convert the output of to_primitive() to a suitable value."""
        return value

    @enforce_nullable
    def from_primitive(self, obj, attr, value):
        """Deserialize a value from primitive form.

        This is responsible for deserializing a value from primitive into
        regular form. In the base class, it handles the potentially nullable
        nature of the field and calls a helper of self._from_primitive(value)
        to do the actual deserialization.

        :param:obj: The object being acted upon
        :param:attr: The name of the attribute/field being deserialized
        :param:value: The value to be deserialized
        :returns: The deserialized value
        """
        if value is None:
            return None
        else:
            return self._from_primitive(value)

    def _to_primitive(self, value):
        """Convert a value to a primitive suitable for transport."""
        return value

    @enforce_nullable
    def to_primitive(self, obj, attr, value):
        """Serialize a value to primitive form.

        This is responsible for serializing a value to primitive form. In
        the base class, it handles the potentially nullable nature of the
        field, and calls a helper of self._to_primitive(value) to do the
        actual serialization.

        :param:obj: The object being acted upon
        :param:attr: The name of the attribute/field being serialized
        :param:value: The value to be serialized
        :returns: The serialized value
        """
        if value is None:
            return None
        else:
            return self._to_primitive(value)

    def describe(self):
        """Return a short string describing the type of this field."""
        name = self.__class__.__name__.replace('Field', '')
        if not name:
            name = 'Null'
        prefix = self._nullable and 'Nullable' or ''
        return prefix + name


class StringField(Field):
    def _coerce(self, value):
        return unicode(value)


class IntegerField(Field):
    def _coerce(self, value):
        return int(value)


class BooleanField(Field):
    def _coerce(self, value):
        return bool(value)


class DateTimeField(Field):
    def _coerce(self, value):
        if isinstance(value, basestring):
            value = timeutils.parse_isotime(value)
        elif not isinstance(value, datetime.datetime):
            raise ValueError('A datetime.datetime is required here')

        if value.utcoffset() is None:
            value = value.replace(tzinfo=iso8601.iso8601.Utc())
        return value

    def _from_primitive(self, value):
        return timeutils.parse_isotime(value)

    def _to_primitive(self, value):
        return timeutils.isotime(value)


class IPAddressField(Field):
    def __init__(self, **kwargs):
        self._version = int(kwargs.pop('version', 4))
        super(IPAddressField, self).__init__(**kwargs)

    def _coerce(self, value):
        return netaddr.IPAddress(value, version=self._version)

    def _from_primitive(self, value):
        return self._coerce(value)

    def _to_primitive(self, value):
        return str(value)


class ListField(Field):
    def __init__(self, element_type, **kwargs):
        self._element_type = element_type
        super(ListField, self).__init__(**kwargs)

    def _coerce(self, value):
        if not isinstance(value, list):
            raise ValueError(_('A list is required here'))
        for element in value:
            if not isinstance(element, self._element_type):
                raise ValueError(_('Elements must be of type %s') %
                                 self._element_type)
        return value


class DictField(Field):
    def __init__(self, element_type, **kwargs):
        self._element_type = element_type
        super(DictField, self).__init__(**kwargs)

    def _coerce(self, value):
        if not isinstance(value, dict):
            raise ValueError(_('A dict is required here'))
        for key, value in value.items():
            if not isinstance(key, basestring):
                raise ValueError(_('Key %s is not a string') % repr(key))
            if not isinstance(value, self._element_type):
                raise ValueError(_('Elements must be of type %s') %
                                 self._element_type)
        return value


class ObjectField(Field):
    def __init__(self, objtype, **kwargs):
        self._objtype = objtype
        super(ObjectField, self).__init__(**kwargs)

    def _coerce(self, value):
        if not isinstance(value, self._objtype):
            raise ValueError(_('An object of type %s is required here') %
                             self._objtype.objname())

    def _to_primitive(self, value):
        if hasattr(value, '__iter__'):
            return [x.obj_to_primitive() for x in value]
        else:
            return value.obj_to_primitive()

    def _from_primitive(self, value):
        # FIXME(danms): Avoid circular import from base.py
        from nova.objects import base as obj_base

        if isinstance(value, list):
            # We're hydrating a ObjectListBase thing
            listobj = self._objtype()
            for item in value:
                listobj.append(obj_base.NovaObject.obj_from_primitive(item))
            return listobj
        else:
            return obj_base.NovaObject.obj_from_primitive(value)


class NetworkModelField(Field):
    def _coerce(self, value):
        if isinstance(value, network_model.NetworkInfo):
            return value
        elif isinstance(value, basestring):
            # Hmm, do we need this?
            return network_model.NetworkInfo.hydrate(value)
        else:
            raise ValueError(_('A NetworkModel is required here'))

    def _to_primitive(self, value):
        return value.json()

    def _from_primitive(self, value):
        return network_model.NetworkInfo.hydrate(value)

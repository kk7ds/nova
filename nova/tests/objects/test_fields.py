import datetime
import iso8601
import netaddr

from nova.objects import fields
from nova.openstack.common import timeutils
from nova import test


class FakeField(fields.Field):
    def _coerce(self, value):
        return 'foo!'

    def _to_primitive(self, value):
        return {'value': value}

    def _from_primitive(self, value):
        return value['value']


class TestField(test.TestCase):
    def test_coerce(self):
        field = FakeField()
        self.assertEqual('foo!', field.coerce(None, None, 'bar'))
        self.assertRaises(ValueError, field.coerce, None, None, None)

    def test_coerce_nullable(self):
        field = FakeField(nullable=True)
        self.assertEqual('foo!', field.coerce(None, None, 'bar'))
        self.assertEqual(None, field.coerce(None, None, None))

    def test_to_primitive(self):
        field = FakeField()
        self.assertEqual({'value': 'foo'},
                         field.to_primitive(None, None, 'foo'))

    def test_from_primitive(self):
        field = FakeField()
        self.assertEqual('foo',
                         field.from_primitive(None, None, {'value': 'foo'}))

    def test_describe(self):
        field1 = FakeField()
        field2 = FakeField(nullable=True)
        self.assertEqual('Fake', field1.describe())
        self.assertEqual('NullableFake', field2.describe())


class TestBaseFields(test.TestCase):
    def test_string(self):
        field = fields.StringField()
        self.assertEqual('foo', field.coerce(None, None, 'foo'))
        self.assertEqual('1', field.coerce(None, None, 1))
        self.assertEqual('foo',
                         field.from_primitive(None, None,
                                              field.to_primitive(None, None,
                                                                 'foo')))

    def test_integer(self):
        field = fields.IntegerField()
        self.assertEqual(1, field.coerce(None, None, 1))
        self.assertEqual(1, field.coerce(None, None, '1'))
        self.assertEqual(1,
                         field.from_primitive(None, None,
                                              field.to_primitive(None, None,
                                                                 1)))

    def test_boolean(self):
        field = fields.BooleanField()
        self.assertEqual(True, field.coerce(None, None, True))
        self.assertEqual(False, field.coerce(None, None, False))
        self.assertEqual(True, field.coerce(None, None, 'foobar'))
        self.assertEqual(False, field.coerce(None, None, 0))
        self.assertEqual(True,
                         field.from_primitive(None, None,
                                              field.to_primitive(None, None,
                                                                 True)))

class TestDateTimeField(test.TestCase):
    def setUp(self):
        super(TestDateTimeField, self).setUp()
        self.field = fields.DateTimeField()
        self.dt = datetime.datetime(1955, 11, 5, tzinfo=iso8601.iso8601.Utc())

    def test_datetime_coercion(self):
        isotime = timeutils.isotime(self.dt)
        self.assertEqual(self.dt, self.field.coerce(None, None, self.dt))
        self.assertEqual(self.dt, self.field.coerce(None, None, isotime))
        self.assertRaises(ValueError, self.field.coerce, None, None, 1)

    def test_datetime_serialization(self):
        self.assertEqual(timeutils.isotime(self.dt),
                         self.field.to_primitive(None, None, self.dt))

    def test_datetime_deserialization(self):
        primitive = self.field.to_primitive(None, None, self.dt)
        self.assertEqual(self.dt,
                         self.field.from_primitive(None, None, primitive))


class TestIPAddressField(test.TestCase):
    def setUp(self):
        super(TestIPAddressField, self).setUp()
        self.field = fields.IPAddressField()

    def test_ipaddress_coercion(self):
        ip = netaddr.IPAddress('1.2.3.4')
        self.assertEqual(ip, self.field.coerce(None, None, ip))
        self.assertEqual(ip, self.field.coerce(None, None, str(ip)))
        self.assertRaises(netaddr.AddrFormatError,
                          self.field.coerce, None, None, 'foo')

    def test_ipaddress_serialization(self):
        self.assertEqual('1.2.3.4',
                         self.field.to_primitive(None, None,
                                                 netaddr.IPAddress('1.2.3.4')))

    def test_ipaddress_deserialization(self):
        ip = netaddr.IPAddress('1.2.3.4')
        self.assertEqual(ip,
                         self.field.from_primitive(None, None, str(ip)))

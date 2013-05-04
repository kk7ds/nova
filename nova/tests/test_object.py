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
import gettext
import netaddr

gettext.install('nova')

from nova import context
from nova import db
from nova.object import base
from nova.object import instance
from nova.object import migration
from nova.openstack.common import timeutils
from nova import test


class MyObj(base.NovaObject):
    version = '1.5'
    fields = {'foo': int,
              'bar': str,
              }

    def load(self, attrname):
        setattr(self, attrname, 'loaded!')

    @base.magic_static
    def get(cls, context):
        obj = cls()
        obj.foo = 1
        obj.bar = 'bar'
        obj.reset_changes()
        return obj


class TestMetaclass(test.TestCase):
    def test_obj_tracking(self):

        class NewBaseClass(object):
            __metaclass__ = base.NovaObjectMetaclass
            fields = {}

            @classmethod
            def objname(cls):
                return cls.__name__

        class Test1(NewBaseClass):
            @staticmethod
            def objname():
                return 'fake1'

        class Test2(NewBaseClass):
            pass

        expected = {'fake1': Test1, 'Test2': Test2}

        self.assertEqual(expected, NewBaseClass._obj_classes)
        # The following should work, also.
        self.assertEqual(expected, Test1._obj_classes)
        self.assertEqual(expected, Test2._obj_classes)


class TestObject(test.TestCase):
    #def setUp(self):
    #    base.are_things_remote = False

    def test_hydration_type_error(self):
        primitive = {'nova_object.name': 'MyObj',
                     'nova_object.data': {'foo': 'a'}}
        self.assertRaises(ValueError, MyObj.from_primitive, primitive)

    def test_hydration(self):
        primitive = {'nova_object.name': 'MyObj',
                     'nova_object.data': {'foo': 1}}
        obj = MyObj.from_primitive(primitive)
        self.assertEqual(obj.foo, 1)

    def test_dehydration(self):
        expected = {'nova_object.name': 'MyObj',
                    'nova_object.data': {'foo': 1}}
        obj = MyObj()
        obj.foo = 1
        obj.reset_changes()
        self.assertEqual(obj.to_primitive(), expected)

    def test_object_property(self):
        obj = MyObj()
        obj.foo = 1
        self.assertEqual(obj.foo, 1)

    def test_object_property_type_error(self):
        obj = MyObj()

        def fail():
            obj.foo = 'a'
        self.assertRaises(ValueError, fail)

    def test_object_dict_syntax(self):
        obj = MyObj()
        obj.foo = 123
        self.assertEqual(obj['foo'], 123)

    def test_load(self):
        obj = MyObj()
        self.assertEqual(obj.bar, 'loaded!')

    def test_loaded_in_primitive(self):
        obj = MyObj()
        obj.foo = 1
        obj.reset_changes()
        self.assertEqual(obj.bar, 'loaded!')
        expected = {'nova_object.name': 'MyObj',
                    'nova_object.data': {'foo': 1,
                                         'bar': 'loaded!'}}
        self.assertEqual(obj.to_primitive(), expected)

    def test_changes_in_primitive(self):
        mig = migration.Migration()
        mig.id = 123
        self.assertEqual(mig.what_changed(), set(['id']))
        primitive = mig.to_primitive()
        self.assertTrue('nova_object.changes' in primitive)
        mig2 = migration.Migration.from_primitive(primitive)
        self.assertEqual(mig2.what_changed(), set(['id']))
        mig2.reset_changes()
        self.assertEqual(mig2.what_changed(), set())

    def _testable_conductor(self):
        conductor_service = self.start_service(
            'conductor', manager='nova.conductor.manager.ConductorManager')

        orig_object_class_action = \
            conductor_service.manager.object_class_action
        orig_object_action = \
            conductor_service.manager.object_action

        def fake_object_class_action(*args, **kwargs):
            # Temporarily go non-remote so the conductor handles
            # this request directly
            base.are_things_remote = False
            result = orig_object_class_action(*args, **kwargs)
            base.are_things_remote = True
            return result
        self.stubs.Set(conductor_service.manager, 'object_class_action',
                       fake_object_class_action)

        def fake_object_action(*args, **kwargs):
            # Temporarily go non-remote so the conductor handles
            # this request directly
            base.are_things_remote = False
            result = orig_object_action(*args, **kwargs)
            base.are_things_remote = True
            return result

        # Things are remoted by default in this session
        base.are_things_remote = True

    def test_base_remote(self):
        self._testable_conductor()
        ctxt = context.get_admin_context()
        obj = MyObj.get(ctxt)
        self.assertEqual(obj.bar, 'bar')
        # FIXME: verify that it was actually done remotely

    def _prepare_for_version_permutation(self):
        class MyObj2(MyObj):
            pass

        # _After_ we're registered, start evilishly returning our name as
        # that of another object so that the client will claim to instantiate
        # MyObj with our version, which won't actually match what is in the
        # registry
        MyObj2.objname = classmethod(lambda c: 'MyObj')

        self._testable_conductor()
        return MyObj2

    def test_remote_major_version_mismatch(self):
        ctxt = context.get_admin_context()
        MyObj = self._prepare_for_version_permutation()
        MyObj.version = '2.0'
        self.assertRaises(base.IncompatibleObjectMajorVersion, MyObj.get, ctxt)

    def test_remote_minor_version_greater(self):
        ctxt = context.get_admin_context()
        MyObj = self._prepare_for_version_permutation()
        MyObj.version = '1.6'
        self.assertRaises(base.IncompatibleObjectMinorVersion, MyObj.get, ctxt)

    def test_remote_minor_version_less(self):
        ctxt = context.get_admin_context()
        MyObj = self._prepare_for_version_permutation()
        MyObj.version = '1.2'
        obj = MyObj.get(ctxt)
        self.assertEqual(obj.bar, 'bar')


class TestMigrationObject(test.TestCase):
    def test_hydration(self):
        mig = migration.Migration()
        mig.id = 123
        mig.source_compute = 'foo'
        mig.dest_compute = 'bar'
        mig.source_node = 'foonode'
        mig.dest_node = 'barnode'
        mig.dest_host = 'baz'
        mig.old_instance_type_id = 1
        mig.new_instance_type_id = 2
        mig.instance_uuid = 'fake-uuid'
        mig.status = 'some status...'

        mig2 = base.NovaObject.from_primitive(mig.to_primitive())

        self.assertEqual(dict(mig.iteritems()),
                         dict(mig2.iteritems()))

    def test_get(self):
        base.are_things_remote = False
        ctxt = None

        def fake_get(context, migration_id):
            return dict(id=migration_id,
                        source_compute='foo',
                        dest_compute='bar',
                        source_node='foonode',
                        dest_node='barnode',
                        dest_host='foobar',
                        old_instance_type_id=1,
                        new_instance_type_id=2,
                        instance_uuid='fake-uuid',
                        status='none yet')
        self.stubs.Set(db, 'migration_get', fake_get)

        mig = migration.Migration.get(ctxt, migration_id=123)
        self.assertEqual(mig.id, 123)

    def test_get_remote(self):
        base.are_things_remote = True

        ctxt = context.get_admin_context()
        mig = migration.Migration()
        mig.id = 123
        mig.source_compute = 'foo'
        mig.dest_compute = 'bar'
        mig.source_node = 'foonode'
        mig.dest_node = 'barnode'
        mig.dest_host = 'baz'
        mig.old_instance_type_id = 1
        mig.new_instance_type_id = 2
        mig.instance_uuid = 'fake-uuid'
        mig.status = 'some status...'

        conductor_service = self.start_service(
            'conductor', manager='nova.conductor.manager.ConductorManager')

        self.mox.StubOutWithMock(conductor_service.manager,
                                 'object_class_action')
        self.mox.StubOutWithMock(conductor_service.manager,
                                 'object_action')

        conductor_service.manager.object_class_action(
            ctxt, objname='Migration', objmethod='get', objver='1.0',
            migration_id=123).AndReturn(mig)

        conductor_service.manager.object_action(
            ctxt, objinst=mig.to_primitive(), objmethod='save',
            objver='1.0').AndReturn(None)

        self.mox.ReplayAll()

        rmig = migration.Migration.get(ctxt, migration_id=123)
        self.assertEqual(rmig.id, mig.id)
        self.assertEqual(rmig.source_node, mig.source_node)

        rmig.save(ctxt)


class TestInstanceObject(test.TestCase):
    def test_datetime_hydration(self):
        red_letter_date = timeutils.parse_isotime(
            timeutils.isotime(datetime.datetime(1955, 11, 5)))
        inst = instance.Instance()
        inst.uuid = 'fake-uuid'
        inst.launched_at = red_letter_date
        primitive = inst.to_primitive()
        expected = {'nova_object.name': 'Instance',
                    'nova_object.data':
                        {'uuid': 'fake-uuid',
                         'launched_at': '1955-11-05T00:00:00Z'},
                    'nova_object.changes': ['uuid', 'launched_at']}
        self.assertEqual(primitive, expected)
        inst2 = instance.Instance.from_primitive(primitive)
        self.assertTrue(isinstance(inst2.launched_at,
                        datetime.datetime))
        self.assertEqual(inst2.launched_at, red_letter_date)

    def test_ip_hydration(self):
        inst = instance.Instance()
        inst.uuid = 'fake-uuid'
        inst.access_ip_v4 = '1.2.3.4'
        inst.access_ip_v6 = '::1'
        primitive = inst.to_primitive()
        expected = {'nova_object.name': 'Instance',
                    'nova_object.data':
                        {'uuid': 'fake-uuid',
                         'access_ip_v4': '1.2.3.4',
                         'access_ip_v6': '::1'},
                    'nova_object.changes': ['uuid', 'access_ip_v6',
                                            'access_ip_v4']}
        self.assertEqual(primitive, expected)
        inst2 = instance.Instance.from_primitive(primitive)
        self.assertTrue(isinstance(inst2.access_ip_v4, netaddr.IPAddress))
        self.assertTrue(isinstance(inst2.access_ip_v6, netaddr.IPAddress))
        self.assertEqual(inst2.access_ip_v4, netaddr.IPAddress('1.2.3.4'))
        self.assertEqual(inst2.access_ip_v6, netaddr.IPAddress('::1'))

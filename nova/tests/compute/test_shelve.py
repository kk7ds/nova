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

import mox

from nova.compute import task_states
from nova.compute import vm_states
from nova import db
from nova.openstack.common import jsonutils
from nova.openstack.common import timeutils
from nova.tests.compute import test_compute
from nova.tests.image import fake as fake_image
from nova import utils


class ShelveComputeManagerTestCase(test_compute.BaseTestCase):
    def test_shelve(self):
        instance = jsonutils.to_primitive(self._create_fake_instance())
        self.compute.run_instance(self.context, instance=instance)
        instance = db.instance_update(self.context, instance['uuid'],
                {"task_state": task_states.SHELVING})
        image_id = 'fake_image_id'
        host = 'fake-mini'
        cur_time = timeutils.utcnow()
        timeutils.set_time_override(cur_time)
        sys_meta = utils.metadata_to_dict(instance['system_metadata'])
        sys_meta['shelved_at'] = timeutils.strtime(at=cur_time)
        sys_meta['shelved_image_id'] = image_id
        sys_meta['shelved_host'] = host

        self.mox.StubOutWithMock(self.compute, '_notify_about_instance_usage')
        self.mox.StubOutWithMock(self.compute.driver, 'snapshot')
        self.mox.StubOutWithMock(self.compute.driver, 'power_off')
        self.mox.StubOutWithMock(self.compute, '_get_power_state')
        self.mox.StubOutWithMock(db, 'instance_update_and_get_original')

        self.compute._notify_about_instance_usage(self.context, instance,
                'shelve.start')
        self.compute.driver.power_off(instance)
        self.compute._get_power_state(self.context,
                instance).AndReturn('fake_power')
        self.compute.driver.snapshot(self.context, instance, 'fake_image_id',
                mox.IgnoreArg())

        db.instance_update_and_get_original(self.context, instance['uuid'],
                {'power_state': 'fake_power',
                 'vm_state': vm_states.SHELVED,
                 'task_state': None,
                 'expected_task_state': [task_states.SHELVING,
                    task_states.SHELVING_IMAGE_UPLOADING],
                 'system_metadata': sys_meta}).AndReturn((instance, instance))
        self.compute._notify_about_instance_usage(self.context,
                jsonutils.to_primitive(instance), 'shelve.end')
        self.mox.ReplayAll()

        self.compute.shelve_instance(self.context, instance,
                image_id=image_id)

        self.mox.VerifyAll()
        self.mox.UnsetStubs()

        self.compute.terminate_instance(self.context, instance=instance)

    def test_shelve_volume_backed(self):
        instance = jsonutils.to_primitive(self._create_fake_instance())
        self.compute.run_instance(self.context, instance=instance)
        instance = db.instance_update(self.context, instance['uuid'],
                {"task_state": task_states.SHELVING})
        instance = jsonutils.to_primitive(instance)
        host = 'fake-mini'
        cur_time = timeutils.utcnow()
        timeutils.set_time_override(cur_time)
        sys_meta = utils.metadata_to_dict(instance['system_metadata'])
        sys_meta['shelved_at'] = timeutils.strtime(at=cur_time)
        sys_meta['shelved_image_id'] = None
        sys_meta['shelved_host'] = host

        self.mox.StubOutWithMock(self.compute, '_notify_about_instance_usage')
        self.mox.StubOutWithMock(self.compute.driver, 'power_off')
        self.mox.StubOutWithMock(self.compute, '_get_power_state')
        self.mox.StubOutWithMock(db, 'instance_update_and_get_original')

        self.compute._notify_about_instance_usage(self.context, instance,
                'shelve_offload.start')
        self.compute.driver.power_off(instance)
        self.compute._get_power_state(self.context,
                instance).AndReturn('fake_power')
        db.instance_update_and_get_original(self.context, instance['uuid'],
                {'power_state': 'fake_power', 'host': None, 'node': None,
                 'vm_state': vm_states.SHELVED_OFFLOADED,
                 'task_state': None,
                 'expected_task_state': [task_states.SHELVING,
                    task_states.SHELVING_OFFLOADING]}).AndReturn(
                            (instance, instance))
        self.compute._notify_about_instance_usage(self.context, instance,
                'shelve_offload.end')
        self.mox.ReplayAll()

        self.compute.shelve_offload_instance(self.context, instance)

        self.mox.VerifyAll()
        self.mox.UnsetStubs()

        self.compute.terminate_instance(self.context, instance=instance)

    def test_unshelve(self):
        instance = jsonutils.to_primitive(self._create_fake_instance())
        self.compute.run_instance(self.context, instance=instance)
        instance = jsonutils.to_primitive(db.instance_update(self.context,
            instance['uuid'], {"task_state": task_states.UNSHELVING}))
        image = {'id': 'fake_id'}
        host = 'fake-mini'
        cur_time = timeutils.utcnow()
        timeutils.set_time_override(cur_time)
        sys_meta = utils.metadata_to_dict(instance['system_metadata'])
        sys_meta['shelved_at'] = timeutils.strtime(at=cur_time)
        sys_meta['shelved_image_id'] = image['id']
        sys_meta['shelved_host'] = host

        self.mox.StubOutWithMock(self.compute, '_notify_about_instance_usage')
        self.mox.StubOutWithMock(self.compute, '_prep_block_device')
        self.mox.StubOutWithMock(self.compute.driver, 'spawn')
        self.mox.StubOutWithMock(db, 'instance_update_and_get_original')

        self.deleted_image_id = None

        def fake_delete(self2, ctxt, image_id):
            self.deleted_image_id = image_id

        fake_image.stub_out_image_service(self.stubs)
        self.stubs.Set(fake_image._FakeImageService, 'delete', fake_delete)

        self.compute._notify_about_instance_usage(self.context, instance,
                'unshelve.start')
        db.instance_update_and_get_original(self.context, instance['uuid'],
                {'task_state': task_states.SPAWNING}).AndReturn(
                        (instance, instance))
        self.compute._prep_block_device(self.context, instance,
                []).AndReturn('fake_bdm')
        instance['key_data'] = None
        instance['auto_disk_config'] = None
        self.compute.driver.spawn(self.context, instance, image,
                injected_files=[], admin_password=None,
                network_info=[],
                block_device_info='fake_bdm')
        db.instance_update_and_get_original(self.context, instance['uuid'],
                {'power_state': 1,
                 'vm_state': vm_states.ACTIVE,
                 'task_state': None,
                 'expected_task_state': task_states.SPAWNING,
                 'launched_at': cur_time}).AndReturn((instance, instance))
        self.compute._notify_about_instance_usage(self.context, instance,
                'unshelve.end')
        self.mox.ReplayAll()

        instance['key_data'] = 'fake_key'
        instance['auto_disk_config'] = True
        self.compute.unshelve_instance(self.context, instance,
                image=image)
        self.assertEqual(image['id'], self.deleted_image_id)

        self.mox.VerifyAll()
        self.mox.UnsetStubs()

        self.compute.terminate_instance(self.context, instance=instance)

    def test_unshelve_volume_backed(self):
        instance = jsonutils.to_primitive(self._create_fake_instance())
        self.compute.run_instance(self.context, instance=instance)
        instance = jsonutils.to_primitive(db.instance_update(self.context,
            instance['uuid'], {"task_state": task_states.UNSHELVING}))
        host = 'fake-mini'
        cur_time = timeutils.utcnow()
        timeutils.set_time_override(cur_time)
        sys_meta = utils.metadata_to_dict(instance['system_metadata'])
        sys_meta['shelved_at'] = timeutils.strtime(at=cur_time)
        sys_meta['shelved_image_id'] = None
        sys_meta['shelved_host'] = host

        self.mox.StubOutWithMock(self.compute, '_notify_about_instance_usage')
        self.mox.StubOutWithMock(self.compute, '_prep_block_device')
        self.mox.StubOutWithMock(self.compute.driver, 'spawn')
        self.mox.StubOutWithMock(db, 'instance_update_and_get_original')

        self.compute._notify_about_instance_usage(self.context, instance,
                'unshelve.start')
        db.instance_update_and_get_original(self.context, instance['uuid'],
                {'task_state': task_states.SPAWNING}).AndReturn(
                        (instance, instance))
        self.compute._prep_block_device(self.context, instance,
                []).AndReturn('fake_bdm')
        instance['key_data'] = None
        instance['auto_disk_config'] = None
        self.compute.driver.spawn(self.context, instance, None,
                injected_files=[], admin_password=None,
                network_info=[],
                block_device_info='fake_bdm')
        db.instance_update_and_get_original(self.context, instance['uuid'],
                {'power_state': 1,
                 'vm_state': vm_states.ACTIVE,
                 'task_state': None,
                 'expected_task_state': task_states.SPAWNING,
                 'launched_at': cur_time}).AndReturn((instance, instance))
        self.compute._notify_about_instance_usage(self.context, instance,
                'unshelve.end')
        self.mox.ReplayAll()

        instance['key_data'] = 'fake_key'
        instance['auto_disk_config'] = True
        self.compute.unshelve_instance(self.context, instance, image=None)

        self.mox.VerifyAll()
        self.mox.UnsetStubs()

        self.compute.terminate_instance(self.context, instance=instance)


class ShelveComputeAPITestCase(test_compute.BaseTestCase):
    def test_shelve(self):
        # Ensure instance can be shelved.
        instance = jsonutils.to_primitive(self._create_fake_instance())
        instance_uuid = instance['uuid']
        self.compute.run_instance(self.context, instance=instance)

        self.assertEqual(instance['task_state'], None)

        self.compute_api.shelve(self.context, instance)

        instance = db.instance_get_by_uuid(self.context, instance_uuid)
        self.assertEqual(instance['task_state'], task_states.SHELVING)

        db.instance_destroy(self.context, instance['uuid'])

    def test_unshelve(self):
        # Ensure instance can be unshelved.
        instance = jsonutils.to_primitive(self._create_fake_instance())
        instance_uuid = instance['uuid']
        self.compute.run_instance(self.context, instance=instance)

        self.assertEqual(instance['task_state'], None)

        self.compute_api.shelve(self.context, instance)
        instance = db.instance_update(self.context, instance['uuid'],
                {'task_state': None, 'vm_state': vm_states.SHELVED})

        self.compute_api.unshelve(self.context, instance)

        instance = db.instance_get_by_uuid(self.context, instance_uuid)
        self.assertEqual(instance['task_state'], task_states.UNSHELVING)

        db.instance_destroy(self.context, instance['uuid'])

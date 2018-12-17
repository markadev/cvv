from copy import deepcopy

import pytest

from cvv.replica import ConcurrentUpdateException, DuplicateKeyException, \
    NoSuchKeyException, Replica
from cvv.vtypes import Version, VersionVector


class FakeMessageBus:
    def __init__(self):
        self.members = {}

    def add_member(self, member_id, member):
        self.members[member_id] = (member, [])

    def broadcast(self, sender_id, msg):
        """Enqueues a message to all members except the sender."""
        for member_id, v in self.members.items():
            member, q = v[0:2]
            if member_id != sender_id:
                v[1].append((sender_id, deepcopy(msg)))

    def send(self, sender_id, dest_id, msg):
        """Enqueues a message to the given recipient."""
        try:
            q = self.members[dest_id][1]
            q.append((sender_id, msg))
        except KeyError:
            pass

    def reorder(self, dest_id):
        q = self.members[dest_id][1]
        q.reverse()

    def drop_all_messages(self):
        for v in self.members.values():
            q = v[1]
            del q[:]

    def deliver_one(self, member_id):
        member, q = self.members[member_id][0:2]
        sender_id, msg = q.pop(0)
        member.deliver_message(sender_id, msg)

    def deliver_all(self):
        for v in self.members.values():
            member, q = v[0:2]
            while len(q) > 0:
                sender_id, msg = q.pop(0)
                member.deliver_message(sender_id, msg)


@pytest.fixture
def msg_bus():
    return FakeMessageBus()


@pytest.fixture
def r1(msg_bus):
    replica = Replica('AA', msg_bus)
    msg_bus.add_member(replica.replica_id, replica)
    return replica


@pytest.fixture
def r2(msg_bus):
    replica = Replica('BB', msg_bus)
    msg_bus.add_member(replica.replica_id, replica)
    return replica


@pytest.fixture
def r3(msg_bus):
    replica = Replica('CC', msg_bus)
    msg_bus.add_member(replica.replica_id, replica)
    return replica


def test_create_single(msg_bus, r1, r2):
    expected_dependents = VersionVector()
    expected_dependents.inc_version(r1.replica_id)

    # Create object on r1, do not propagate messages yet
    r1.create('place', 'chicago')
    r1_v = r1.read('place')
    r2_v = r2.read('place')
    assert r1_v.dependent_versions == expected_dependents
    assert r1_v.values == ['chicago']
    assert r2_v.dependent_versions == VersionVector()
    assert r2_v.values == []

    # Replicate to r2
    msg_bus.deliver_all()
    r2_v = r2.read('place')
    assert r2_v.dependent_versions == r1_v.dependent_versions
    assert r2_v.values == r1_v.values


def test_create_conflict(msg_bus, r1, r2, r3):
    expected_dependents_r1 = VersionVector()
    expected_dependents_r1.inc_version(r1.replica_id)
    expected_dependents_r2 = VersionVector()
    expected_dependents_r2.inc_version(r2.replica_id)
    expected_dependents_r1r2 = VersionVector()
    expected_dependents_r1r2.update(expected_dependents_r1)
    expected_dependents_r1r2.update(expected_dependents_r2)

    # Create object on r1 and r2 but do not propagate messages
    r1.create('place', 'chicago')
    r2.create('place', 'munich')

    r1_v = r1.read('place')
    assert r1_v.dependent_versions == expected_dependents_r1
    assert r1_v.values == ['chicago']
    r2_v = r2.read('place')
    assert r2_v.dependent_versions == expected_dependents_r2
    assert r2_v.values == ['munich']

    # Replicate around
    msg_bus.deliver_all()

    # Verify all replicas
    for replica in (r1, r2, r3):
        rres = replica.read('place')
        assert rres.dependent_versions == expected_dependents_r1r2
        assert sorted(rres.values) == ['chicago', 'munich']


def test_create_disallow_known_conflict(msg_bus, r1, r2, r3):
    # Create object on r1 and replicate
    r1.create('place', 'philadelphia')
    msg_bus.deliver_all()

    with pytest.raises(DuplicateKeyException):
        r1.create('place', 'stockholm')

    with pytest.raises(DuplicateKeyException):
        r2.create('place', 'stockholm')

    with pytest.raises(DuplicateKeyException):
        r3.create('place', 'stockholm')


def test_create_many(msg_bus, r1, r2, r3):
    r1.create('ed.home', 'downtown')
    r1.create('ed.food', 'taquitos')
    r2.create('bob.home', 'uptown')
    r2.create('bob.food', 'fish')
    msg_bus.deliver_all()
    r3.create('jim.home', 'lefttown')
    r3.create('jim.food', 'steak')
    msg_bus.deliver_all()

    for replica in (r1, r2, r3):
        assert replica.read('ed.home').values == ['downtown']
        assert replica.read('ed.food').values == ['taquitos']
        assert replica.read('bob.home').values == ['uptown']
        assert replica.read('bob.food').values == ['fish']
        assert replica.read('jim.home').values == ['lefttown']
        assert replica.read('jim.food').values == ['steak']


def test_update_known_nonexistant(r1):
    with pytest.raises(NoSuchKeyException):
        r1.update('fakekey', 'the_value', VersionVector())


def test_update_invalid_dependent_versions(msg_bus, r1):
    r1.create('key1', 'value1')

    # Create dependent version vector from THE FUTURE
    dependents = VersionVector()
    dependents.update_version(Version(r1.replica_id, 20))
    with pytest.raises(ValueError):
        r1.update('key1', 'new_value', dependents)


def test_update_succeeds(msg_bus, r1, r2):
    r1.create('key1', 'value1')
    msg_bus.deliver_all()

    rv = r1.read('key1')
    r1.update('key1', 'new_value', rv.dependent_versions)
    msg_bus.deliver_all()

    for replica in (r1, r2):
        assert replica.read('key1').values == ['new_value']


def test_update_concurrent_on_same_replica(r1):
    r1.create('key1', 'value1')

    # First thread does a read
    rv1 = r1.read('key1')

    # Second thread does read & update
    rv2 = r1.read('key1')
    r1.update('key1', 'new_value_1', rv2.dependent_versions)

    # First thread finally does its update
    with pytest.raises(ConcurrentUpdateException):
        r1.update('key1', 'new_value_2', rv1.dependent_versions)


def test_update_concurrent_on_different_replica(msg_bus, r1, r2):
    r1.create('key1', 'value1')
    msg_bus.deliver_all()

    # Client on first replica does a read
    rv1 = r1.read('key1')

    # Client on second replica does read & update
    rv2 = r2.read('key1')
    r2.update('key1', 'new_value_1', rv2.dependent_versions)
    msg_bus.deliver_all()

    # Client on first replica finally does its update
    with pytest.raises(ConcurrentUpdateException):
        r1.update('key1', 'new_value_2', rv1.dependent_versions)


def test_update_conflicting(msg_bus, r1, r2, r3):
    r1.create('key1', 'value1')
    msg_bus.deliver_all()

    # Create a conflict
    rv1 = r1.read('key1')
    r1.update('key1', 'new_value_1', rv1.dependent_versions)
    rv2 = r2.read('key1')
    r2.update('key1', 'new_value_2', rv2.dependent_versions)
    msg_bus.deliver_all()

    for replica in (r1, r2, r3):
        rres = replica.read('key1')
        assert sorted(rres.values) == ['new_value_1', 'new_value_2']


def test_resolve_conflict(msg_bus, r1, r2, r3):
    r1.create('key1', 'value1')
    msg_bus.deliver_all()

    # Create a conflict
    rv1 = r1.read('key1')
    r1.update('key1', 'new_value_1', rv1.dependent_versions)
    rv2 = r2.read('key1')
    r2.update('key1', 'new_value_2', rv2.dependent_versions)
    msg_bus.deliver_all()

    rv1 = r1.read('key1')
    r1.update('key1', 'new_value_3', rv1.dependent_versions)
    msg_bus.deliver_all()
    for replica in (r1, r2, r3):
        rres = replica.read('key1')
        assert rres.values == ['new_value_3']
        # TODO: validate rres.dependent_versions


def test_delete_nonexistant(r1):
    r1.delete('fakekey', VersionVector())
    # No exception raised!


def test_delete_and_read(msg_bus, r1, r2):
    r1.create('key1', 'value1')
    msg_bus.deliver_all()

    rv = r1.read('key1')
    r1.delete('key1', rv.dependent_versions)
    msg_bus.deliver_all()

    for replica in (r1, r2):
        assert replica.read('key1').values == []


def test_create_after_delete(msg_bus, r1, r2):
    r1.create('key1', 'value1')
    msg_bus.deliver_all()

    rv = r1.read('key1')
    r1.delete('key1', rv.dependent_versions)
    msg_bus.deliver_all()

    r1.create('key1', 'new_value')
    msg_bus.deliver_all()
    for replica in (r1, r2):
        rres = replica.read('key1')
        assert rres.values == ['new_value']
        # TODO: validate rres.dependent_versions


def test_deliver_out_of_order(msg_bus, r1, r2, r3):
    r1.create('key1.1', 'aaa')
    r1.create('key2.1', 'bbb')
    r2.create('key1.2', 'ccc')
    r2.create('key2.2', 'ddd')
    msg_bus.reorder(r3.replica_id)

    # Deliver one at a time
    msg_bus.deliver_one(r3.replica_id)
    assert r3.read('key1.1').values == []
    assert r3.read('key2.1').values == []
    assert r3.read('key1.2').values == []
    assert r3.read('key2.2').values == []

    msg_bus.deliver_one(r3.replica_id)
    assert r3.read('key1.1').values == []
    assert r3.read('key2.1').values == []
    assert r3.read('key1.2').values == ['ccc']
    assert r3.read('key2.2').values == ['ddd']

    msg_bus.deliver_one(r3.replica_id)
    assert r3.read('key1.1').values == []
    assert r3.read('key2.1').values == []
    assert r3.read('key1.2').values == ['ccc']
    assert r3.read('key2.2').values == ['ddd']

    msg_bus.deliver_one(r3.replica_id)
    assert r3.read('key1.1').values == ['aaa']
    assert r3.read('key2.1').values == ['bbb']
    assert r3.read('key1.2').values == ['ccc']
    assert r3.read('key2.2').values == ['ddd']


def test_causal_plus_with_one_object(msg_bus, r1, r2, r3):
    # Create object on r1, replicate to r2
    r1.create('weather', 'rainy')
    msg_bus.deliver_one(r2.replica_id)

    # Read object created by r1 on r2 and update
    rv = r2.read('weather')
    assert rv.values == ['rainy']
    r2.update('weather', 'winterymix', rv.dependent_versions)

    # Reorder updates to r3 and deliver. We must not see 'rainy'
    assert r3.read('weather').values == []
    msg_bus.reorder(r3.replica_id)

    msg_bus.deliver_one(r3.replica_id)
    assert r3.read('weather').values == []

    msg_bus.deliver_one(r3.replica_id)
    assert r3.read('weather').values == ['winterymix']


def test_causal_plus_with_two_objects(msg_bus, r1, r2, r3):
    # Create object on r1, replicate to r2
    r1.create('weather', 'rainy')
    msg_bus.deliver_one(r2.replica_id)

    # Read object created by r1 on r2 and create new object
    assert r2.read('weather').values == ['rainy']
    r2.create('equipment', 'umbrella')

    # Reorder updates to r3 and deliver. We must not see 'equipment'
    # before 'weather'
    assert r3.read('weather').values == []
    assert r3.read('equipment').values == []
    msg_bus.reorder(r3.replica_id)

    msg_bus.deliver_one(r3.replica_id)
    assert r3.read('weather').values == []
    assert r3.read('equipment').values == []

    msg_bus.deliver_one(r3.replica_id)
    assert r3.read('weather').values == ['rainy']
    assert r3.read('equipment').values == ['umbrella']


def test_simple_sync(msg_bus, r1, r2):
    r1.create('location', 'london')
    r1.create('day', 'sunday')
    msg_bus.drop_all_messages()
    msg_bus.deliver_all()
    assert r2.read('location').values == []
    assert r2.read('day').values == []

    r2.request_sync(r1.replica_id)
    msg_bus.deliver_all()  # Deliver request
    msg_bus.deliver_all()  # Deliver responses
    assert r2.read('location').values == ['london']
    assert r2.read('day').values == ['sunday']


def test_sync_conflict(msg_bus, r1, r2, r3):
    # Create a conflict between r1 and r2
    r1.create('location', 'london')
    r2.create('location', 'cambridge')
    msg_bus.deliver_one(r1.replica_id)
    msg_bus.deliver_one(r2.replica_id)
    msg_bus.drop_all_messages()
    for replica in (r1, r2):
        rv = replica.read('location')
        assert sorted(rv.values) == ['cambridge', 'london']

    # r3 requests a sync from r1
    r3.request_sync(r1.replica_id)
    msg_bus.deliver_all()  # Deliver request
    msg_bus.deliver_all()  # Deliver responses
    rv = r3.read('location')
    assert sorted(rv.values) == ['cambridge', 'london']


def test_sync_with_version_gaps(msg_bus, r1, r2, r3):
    # Separate the final object versions by 5
    r1.create('meal', 'chicken piccata')
    r1.create('time', '19:00')
    for _ in range(4):
        rv = r1.read('time')
        r1.update('time', '19:00', rv.dependent_versions)
    r1.create('place', 'ronaldos')
    for _ in range(4):
        rv = r1.read('place')
        r1.update('place', 'ronaldos', rv.dependent_versions)
    msg_bus.drop_all_messages()

    # r3 requests a sync from r1
    r3.request_sync(r1.replica_id)
    msg_bus.deliver_one(r1.replica_id)  # Deliver request
    for _ in range(4):  # Deliver sync responses except the completion msg
        msg_bus.deliver_one(r3.replica_id)

    # Nothing should be visible yet
    assert r3.read('meal').values == []
    assert r3.read('time').values == []
    assert r3.read('place').values == []

    msg_bus.deliver_all()
    assert r3.read('meal').values == ['chicken piccata']
    assert r3.read('time').values == ['19:00']
    assert r3.read('place').values == ['ronaldos']


# vim:set ts=4 sw=4 expandtab:

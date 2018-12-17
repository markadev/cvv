import logging
import random
import threading
from copy import deepcopy

from cvv.vtypes import Version, VersionSet, VersionVector


__all__ = ['Replica',
           'NoSuchKeyException',
           'DuplicateKeyException',
           'ConcurrentUpdateException',
           'ReadTuple']


class NoSuchKeyException(Exception):
    pass


class DuplicateKeyException(Exception):
    pass


class ConcurrentUpdateException(Exception):
    pass


class ReadTuple:
    def __init__(self, dependent_versions=VersionVector(), values=[]):
        self.dependent_versions = dependent_versions
        self.values = values

    def __str__(self):
        return "{ dependent_versions=%s, values=%s }" % \
            (self.dependent_versions, self.values)


class ObjectVersion:
    def __init__(self, version=Version(), timestamp=VersionVector(),
                 value=None):
        self.version = version
        self.timestamp = timestamp
        self.value = value

    def __str__(self):
        return "{ v=%s, ts=%s, %s }" % \
            (self.version, self.timestamp, str(self.value))


class ObjectRecord:
    def __init__(self):
        self.versions = []

    def __str__(self):
        return str(self.versions)


class UpdateMessage:
    def __init__(self, key, obj_ver):
        self.key = key
        self.obj_ver = obj_ver


class SyncRequestMessage:
    """This message is sent between replicas to request a state sync"""
    def __init__(self, cookie, requestor_knowledge):
        self.cookie = cookie
        self.requestor_knowledge = requestor_knowledge


class SyncResponseSetupMessage:
    """This message is sent in response to a sync request to begin the sync."""
    def __init__(self, cookie, server_knowledge, server_visible):
        self.cookie = cookie
        self.server_knowledge = server_knowledge
        self.server_visible = server_visible


class SyncResponseDataMessage:
    """This message contains the data for one object version in a state sync"""
    def __init__(self, cookie, key, obj_ver):
        self.cookie = cookie
        self.key = key
        self.obj_ver = obj_ver


class SyncResponseCompleteMessage:
    """This message marks the end of a complete state sync."""
    def __init__(self, cookie):
        self.cookie = cookie


class SimDataStore:
    """Interface to persistent KV store. Also keeps object references in check
       by always creating copies of the objects read & written"""
    def __init__(self):
        self.data = {}

    def get(self, key):
        try:
            return deepcopy(self.data[key])
        except KeyError:
            return None

    def put(self, key, value):
        self.data[key] = deepcopy(value)

    def erase(self, key):
        del self.data[key]

    def iterkeys(self):
        return self.data.keys()


class Replica:
    def __init__(self, replica_id, msg_bus):
        self.logger = logging.getLogger("Replica-%s" % replica_id)
        self.replica_id = replica_id
        self.msg_bus = msg_bus
        self.update_lock = threading.Lock()

        # TODO Load persistent state
        self.db = SimDataStore()
        self.knowledge = VersionSet()
        self.committed_visible = VersionVector()
        assert self.knowledge.get_version(self.replica_id) == \
            self.committed_visible.get_version(self.replica_id)
        assert self.knowledge.dominates_vv(self.committed_visible)

        self.visible = deepcopy(self.committed_visible)

        # Initialize sync requestor state
        self.sync_in_progress = False
        self.sync_replica = None
        self.sync_cookie = 0
        self.sync_replica_visible = None
        self.sync_replica_knowledge = None

    def read(self, key):
        """Reads the value(s) of the given key. Returns a ReadTuple with
           the values and their associated update dependency versions.

           If there is no object identified by the given key then an empty
           ReadTuple is returned."""
        assert key is not None

        result = ReadTuple()
        obj_record = self.db.get(key)
        if obj_record is None:
            return result

        result.dependent_versions, result.values = \
            self._filter_visible_versions(obj_record)
        # If all the values are tombstones then just return an empty list
        for v in result.values:
            if v is not None:
                # At least one value is not a tombstone
                return result

        return ReadTuple()

    def create(self, key, value):
        """Creates an object in the database identified by the given key
           and containing the given value.

           Returns:
              The version of the object that was created.
           Exceptions:
              DuplicateKeyException - If an object with the given key
                    already exists on this replica."""
        assert key is not None
        assert value is not None

        self.logger.debug("create('%s', %s)", key, value)

        self.update_lock.acquire()
        try:
            obj_record = self.db.get(key)
            if obj_record is not None:
                # We don't require the caller to explicitly give us the
                # dependent versions for the operation. It's a create so there
                # really aren't any dependent versions from the caller's
                # perspective.
                #
                # However, if we are internally storing tombstones then those
                # have to be the dependent versions so the create occurs
                # causally after the previous deletions.
                dependent_versions, visible_values = \
                    self._filter_visible_versions(obj_record)
                for v in visible_values:
                    if v is not None:
                        raise DuplicateKeyException()
            else:
                dependent_versions = VersionVector()
                obj_record = ObjectRecord()
            return self._local_update(obj_record, key, value,
                                      dependent_versions)
        finally:
            self.update_lock.release()

    def update(self, key, value, dependent_versions):
        """Updates the value of the object with the given key. An object
           with the given key must have already been created.

           Parameters:
              key - The identifier of the object to update
              value - The new value of the object
              dependent_versions - The dependent versions returned by the
                    previous read() call for the same key.
           Returns:
              The version of the object that was created.
           Exceptions:
              NoSuchKeyException - If an object with the given key is not found
              ConcurrentUpdateException - If the value of the object has
                    changed since the read() call was performed."""
        assert key is not None
        assert value is not None
        assert type(dependent_versions) is VersionVector

        self.logger.debug("update('%s', %s, %s)", key, value,
            dependent_versions)

        self.update_lock.acquire()
        try:
            obj_record = self.db.get(key)
            if obj_record is None:
                raise NoSuchKeyException()
            return self._local_update(obj_record, key, value,
                                      dependent_versions)
        finally:
            self.update_lock.release()

    def delete(self, key, dependent_versions):
        """Deletes the object identified by the given key.

           Parameters:
              key - The identifier of the object to delete
              dependent_versions - The dependent versions returned by the
                    previous read() call for the same key.

           Exceptions:
              ConcurrentUpdateException - If the value of the object has
                    changed since the read() call was performed."""
        assert key is not None
        assert type(dependent_versions) is VersionVector

        self.logger.debug("delete('%s', %s)", key, dependent_versions)

        self.update_lock.acquire()
        try:
            obj_record = self.db.get(key)
            if obj_record is not None:
                self._local_update(obj_record, key, None, dependent_versions)
        finally:
            self.update_lock.release()

    def request_sync(self, sync_replica_id):
        """Requests a state sync from the given replica."""
        if self.sync_in_progress:
            self.logger.info("Sync from %s already in progress",
                self.sync_replica)
            return

        # Request a sync by sending the peer replica a request with our
        # current knowledge
        self.logger.info("Requesting state sync from %s", sync_replica_id)
        self.sync_replica = sync_replica_id
        self.sync_cookie = random.getrandbits(32)
        self.sync_replica_visible = None
        self.sync_replica_knowledge = None
        self.sync_in_progress = True
        self.msg_bus.send(self.replica_id, sync_replica_id,
            SyncRequestMessage(self.sync_cookie, deepcopy(self.knowledge)))

    def deliver_message(self, sender_id, msg):
        if type(msg) is UpdateMessage:
            self.logger.debug("Processing UpdateMessage from %s", sender_id)
            self._process_update(sender_id, msg)
        elif type(msg) is SyncRequestMessage:
            self.logger.debug("Processing SyncRequestMessage from %s",
                sender_id)
            self._process_sync_request(sender_id, msg)
        elif type(msg) is SyncResponseSetupMessage:
            self.logger.debug("Processing SyncResponseSetupMessage from %s",
                sender_id)
            self._process_sync_response_setup(sender_id, msg)
        elif type(msg) is SyncResponseDataMessage:
            self.logger.debug("Processing SyncResponseDataMessage from %s",
                sender_id)
            self._process_sync_response_data(sender_id, msg)
        elif type(msg) is SyncResponseCompleteMessage:
            self.logger.debug("Processing SyncResponseCompleteMessage from %s",
                sender_id)
            self._process_sync_response_complete(sender_id, msg)
        else:
            self.logger.warn("Received unknown message type from %s",
                sender_id)

    def _process_update(self, sender_id, msg):
        assert type(msg) is UpdateMessage

        self.update_lock.acquire()
        try:
            if self.knowledge.has_version(msg.obj_ver.version):
                # We already have this object
                return

            obj_record = self.db.get(msg.key)
            if obj_record is None:
                obj_record = ObjectRecord()
            self._insert_object(obj_record, msg.key, msg.obj_ver)
        finally:
            self.update_lock.release()

    def _process_sync_request(self, requestor_id, req_msg):
        assert type(req_msg) is SyncRequestMessage
        cookie = req_msg.cookie
        requestor_knowledge = req_msg.requestor_knowledge

        # Send all necessary objects back to the requestor.
        # *** In this simulation we assume that some prefix of these
        # *** messages are delivered in order.
        # We'll use self.committed_visible as our replacement timestamp
        self.msg_bus.send(self.replica_id, requestor_id,
            SyncResponseSetupMessage(cookie,
            deepcopy(self.knowledge), deepcopy(self.committed_visible)))
        for k in self.db.iterkeys():
            obj_record = self.db.get(k)
            discard_timestamp_for_replacement_vv(obj_record,
                self.committed_visible)

            for obj_ver in obj_record.versions:
                if requestor_knowledge.has_version(obj_ver.version):
                    continue
                self.msg_bus.send(self.replica_id, requestor_id,
                    SyncResponseDataMessage(cookie, k, obj_ver))
        self.msg_bus.send(self.replica_id, requestor_id,
            SyncResponseCompleteMessage(cookie))

    def _process_sync_response_setup(self, sender_id, msg):
        if not self.sync_in_progress:
            return
        if sender_id != self.sync_replica or msg.cookie != self.sync_cookie:
            return

        assert type(msg.server_knowledge) is VersionSet
        assert type(msg.server_visible) is VersionVector
        assert msg.server_knowledge.dominates_vv(msg.server_visible)
        self.sync_replica_knowledge = msg.server_knowledge
        self.sync_replica_visible = msg.server_visible

    def _process_sync_response_data(self, sender_id, msg):
        if not self.sync_in_progress:
            return
        if sender_id != self.sync_replica or msg.cookie != self.sync_cookie:
            return
        if self.knowledge.has_version(msg.obj_ver.version):
            return

        assert type(self.sync_replica_knowledge) is VersionSet
        assert type(self.sync_replica_visible) is VersionVector

        if msg.obj_ver.timestamp is None:
            msg.obj_ver.timestamp = deepcopy(self.sync_replica_visible)

        self.update_lock.acquire()
        try:
            obj_record = self.db.get(msg.key)
            if obj_record is None:
                obj_record = ObjectRecord()
            self._insert_object(obj_record, msg.key, msg.obj_ver)
        finally:
            self.update_lock.release()

    def _process_sync_response_complete(self, sender_id, msg):
        if not self.sync_in_progress:
            return
        if sender_id != self.sync_replica or msg.cookie != self.sync_cookie:
            return

        assert type(self.sync_replica_knowledge) is VersionSet
        assert type(self.sync_replica_visible) is VersionVector

        self.logger.info(
            "Sync from %s completed. Merging in knowledge=%s and visible=%s",
            self.sync_replica, self.sync_replica_knowledge,
            self.sync_replica_visible)

        # Merge the server's knowledge into our knowledge. This will
        # fill in version number gaps for versions that the server knew
        # about but no longer exist
        self.update_lock.acquire()
        try:
            self.knowledge.merge(self.sync_replica_knowledge)
            self.visible.update(self.sync_replica_visible)
            self.committed_visible.update(self.visible)
        finally:
            self.update_lock.release()
        self.sync_in_progress = False
        self.sync_replica_knowledge = None
        self.sync_replica_visible = None

    def _filter_visible_versions(self, obj_record):
        """Returns a list of ObjectVersion objects for the visible versions of
           the given Object record."""

        assert self.knowledge.dominates_vv(self.visible)
        assert self.visible.dominates(self.committed_visible)

        # First, filter out non-visible versions. An object o is visible
        # at replica r if r.visible dominates o.version OR r.knowledge
        # dominates o.timestamp. When the second case is true, we also update
        # r.visible so that o and all of its dependencies will be visible.
        # Eventually r.visible will be merged into r.committed_visible.

        visible_versions = []
        for ov in obj_record.versions:
            if self.visible.dominates_version(ov.version):
                visible_versions.append(ov)
            else:
                # visible doesn't dominate, therefore committed_visible
                # won't dominate, therefore the timestamp could not
                # have been optimized out
                assert not self.committed_visible.dominates_version(ov.version)
                assert ov.timestamp is not None

                if self.knowledge.dominates_vv(ov.timestamp):
                    # Latch in a swath of versions as visible
                    self.visible.update(ov.timestamp)
                    visible_versions.append(ov)

        # Now, of the visible versions, filter out the ones that have
        # been replaced by newer versions
        for i in range(len(visible_versions)):
            if visible_versions[i] is None:
                continue
            for j in range(i + 1, len(visible_versions)):
                if visible_versions[j] is None:
                    continue

                # Timestamps must be present because there are multiple
                # versions
                assert visible_versions[i].timestamp is not None
                assert visible_versions[j].timestamp is not None

                if visible_versions[i].timestamp.dominates_version(
                        visible_versions[j].version):
                    visible_versions[j] = None
                elif visible_versions[j].timestamp.dominates_version(
                        visible_versions[i].version):
                    visible_versions[i] = None
                    break

        # Construct our final result
        resulting_values = []
        resulting_vv = VersionVector()
        for ov in visible_versions:
            if ov is None:
                continue
            resulting_values.append(ov.value)

            # There must only be one version for any single replica. Otherwise
            # the replica had somehow conflicted itself.
            assert resulting_vv.get_version(ov.version.replica_id).counter == 0
            resulting_vv.update_version(ov.version)
        return (resulting_vv, resulting_values)

    def _local_update(self, obj_record, key, value, dependent_versions):
        assert type(obj_record) is ObjectRecord
        assert key is not None
        assert type(dependent_versions) is VersionVector
        assert self.update_lock.locked()

        visible_versions = self._filter_visible_versions(obj_record)[0]
        # If the set of versions is different then the update cannot proceed.
        # The caller must resolve the conflict and retry. This is due to the
        # restriction that a replica must create objects that are causally
        # after all objects that it already knows about. (Due to the
        # replica-granularity logical clock)
        if not self.visible.dominates(dependent_versions):
            raise ValueError("Dependent versions from the future")
        if visible_versions != dependent_versions:
            raise ConcurrentUpdateException()

        assert self.knowledge.dominates_vv(self.visible)
        assert self.knowledge.get_version(self.replica_id) == \
            self.visible.get_version(self.replica_id)
        assert self.visible.dominates(self.committed_visible)
        assert self.visible.get_version(self.replica_id) == \
            self.committed_visible.get_version(self.replica_id)

        ver = self.knowledge.get_version(self.replica_id)
        ver.counter += 1
        obj_ver = ObjectVersion(ver, deepcopy(self.visible), value)
        obj_ver.timestamp.update_version(ver)

        obj_ver_copy = deepcopy(obj_ver)
        self._insert_object(obj_record, key, obj_ver)
        assert self.committed_visible.dominates(obj_ver_copy.timestamp)
        self.msg_bus.broadcast(self.replica_id,
            UpdateMessage(key, obj_ver_copy))
        return ver

    def _insert_object(self, obj_record, key, obj_ver):
        """Insert an object and possibly make it visible. Update lock
           must be held"""
        assert type(obj_record) is ObjectRecord
        assert key is not None
        assert type(obj_ver) is ObjectVersion
        assert not self.knowledge.has_version(obj_ver.version)
        assert obj_ver.timestamp is not None
        assert self.update_lock.locked()

        self.logger.debug("Inserting object '%s' version %s, timestamp=%s",
            key, obj_ver.version, obj_ver.timestamp)

        # Reconstruct the timestamps for existing versions while we
        # integrate the new object version
        for ov in obj_record.versions:
            if ov.timestamp is None:
                # It is safe to replace the timestamp with committed_visible
                # because committed_visible satisfies all the constraints
                # for a timestamp that has been discarded
                ov.timestamp = deepcopy(self.committed_visible)

        obj_record.versions.append(obj_ver)
        self.knowledge.insert_version(obj_ver.version)
        if self.knowledge.dominates_vv(obj_ver.timestamp):
            self.visible.update(obj_ver.timestamp)

        # TODO recalculate visible_versions more efficiently
        visible_versions = self._filter_visible_versions(obj_record)[0]

        # Filter out versions no longer needed. A version needs to be
        # retained when:
        #  * It is visible; OR
        #  * It has not yet been made visible
        for i in range(len(obj_record.versions) - 1, -1, -1):
            obj_ver = obj_record.versions[i]
            if obj_ver.version == visible_versions.get_version(
                    obj_ver.version.replica_id):
                # Object version is visible. Keep it!
                continue
            if not self.visible.dominates_version(obj_ver.version):
                # Object version has not yet been made visible
                continue
            del obj_record.versions[i]
        assert len(obj_record.versions) > 0

        discard_timestamp_for_replacement_vv(obj_record, self.visible)

        self.db.put(key, obj_record)
        self.committed_visible.update(self.visible)


# We can discard timestamps, making it eligible to be replaced by 'vv' or
# a later version vector that dominates 'vv' if:
#   1. vv dominates the timestamp; AND
#   2. We assume that vv is Causally Complete (ensured by the caller)
#   3. Replacing the timestamp with vv doesn't change the causal
#      relationship between objects with the same key. Simplify this by
#      saying we'll only discard a timestamp if we are not storing multiple
#      objects with the same key and vv dominates the object's only version
#      (which is implied when check #1 succeeds)
#   4. We assume the local 'knowledge' dominates vv (ensured by the caller).
#      When check #1 also succeeds we can also infer that the local
#      'knowledge' dominates the timestamp
def discard_timestamp_for_replacement_vv(obj_record, vv):
    assert isinstance(obj_record, ObjectRecord)
    assert isinstance(vv, VersionVector)

    if len(obj_record.versions) == 1 and \
            vv.dominates_version(obj_record.versions[0].version):
        obj_record.versions[0].timestamp = None


# vim:set ts=4 sw=4 expandtab:

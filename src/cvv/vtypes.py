__all__ = ['Version', 'VersionVector', 'VersionSet']


class Version:
    def __init__(self, replica_id=None, counter=0):
        self.replica_id = replica_id
        self.counter = counter

    def __eq__(self, other):
        return (self.replica_id == other.replica_id) and \
            (self.counter == other.counter)

    def __str__(self):
        return "{}:{}".format(self.replica_id, self.counter)

    def __repr__(self):
        return "Version({}, {})".format(repr(self.replica_id), self.counter)


class VersionVector:
    def __init__(self):
        # Map of replica ID to version counter. Replicas with no value in this
        # map have a version counter of 0.
        self.v = {}

    def __eq__(self, other):
        return self.v == other.v

    def empty(self):
        """Returns a boolean value indicating if the version vector has
           no versions greater than 0."""
        return len(self.v) == 0

    def dominates(self, other):
        """Checks if this version vector dominates another.

           A version vector (X) dominates another version vector (Y)
           when all versions in X are greater than or equal to all
           versions in Y."""
        assert isinstance(other, VersionVector)

        ids = set(self.v.keys()).union(set(other.v.keys()))
        for id in ids:
            lc = 0
            rc = 0
            if id in self.v:
                lc = self.v[id]
            if id in other.v:
                rc = other.v[id]
            if lc < rc:
                return False
        return True

    def dominates_version(self, ver):
        """Checks if this version vector dominates a single version.

           A version vector (X) dominates a version (Y) when the
           version in X for the replica associated with Y is greater
           than or equal to Y."""
        assert isinstance(ver, Version)

        if ver.replica_id in self.v:
            c = self.v[ver.replica_id]
        else:
            c = 0
        return c >= ver.counter

    def update(self, other):
        """Merges another version vector into this one. The resulting
           version vector will contain the maximum versions from both."""
        assert isinstance(other, VersionVector)

        for replica_id, c in other.v.items():
            self.update_version(Version(replica_id, c))

    def update_version(self, ver):
        """Merges one version into the version vector, updating the
           highest version for the given replica"""
        assert isinstance(ver, Version)

        if ver.replica_id not in self.v:
            self.v[ver.replica_id] = ver.counter
        else:
            self.v[ver.replica_id] = max(self.v[ver.replica_id], ver.counter)

    def get_version(self, replica_id):
        """Gets the value of a single replica version from the version
           vector."""
        try:
            c = self.v[replica_id]
            return Version(replica_id, c)
        except KeyError:
            return Version(replica_id, 0)

    def inc_version(self, replica_id):
        """Increments the value of a single replica verion in the
           version vector."""
        try:
            self.v[replica_id] += 1
        except KeyError:
            self.v[replica_id] = 1
        return Version(replica_id, self.v[replica_id])

    def __str__(self):
        node_ids = self.v.keys()
        result = "[ "
        for nId in sorted(node_ids):
            result += str(Version(nId, self.v[nId]))
            result += " "
        result += "]"
        return result


class VersionSetElement:
    def __init__(self):
        # A range [ 0 - prefix_max ] of contiguous versions
        self.prefix_max = 0
        # A set of non-contiguous versions > prefix_max
        self.extras = set()

    def insert(self, version):
        if version <= self.prefix_max:
            return
        if version == self.prefix_max + 1:
            self.prefix_max = version
            # Remove extras no longer necessary
            self.extras.discard(version)
            self._merge_extras()
        else:
            self.extras.add(version)

    def update_prefix_upper_bound(self, version):
        if version > self.prefix_max:
            self.prefix_max = version
            # Remove extra versions no longer necessary
            self.extras = set(
                filter(lambda x: x > self.prefix_max, self.extras))
            self._merge_extras()

    def insert_extras(self, extras):
        """Add the given extras to this tuple's extra set."""
        self.extras.update(extras)
        # Remove extra versions no longer necessary
        self.extras = set(filter(lambda x: x > self.prefix_max, self.extras))
        self._merge_extras()

    def _merge_extras(self):
        while self.prefix_max + 1 in self.extras:
            self.extras.remove(self.prefix_max + 1)
            self.prefix_max += 1


class VersionSet:
    def __init__(self, iterable=None):
        # Map of replica ID to version set. Replicas with no value in this
        # map have no versions.
        self.v = {}

        if iterable is not None:
            for ver in iterable:
                self.insert_version(ver)

    def empty(self):
        """Returns a boolean value indicating if the set is empty."""
        return len(self.v) == 0

    def get_version(self, replica_id):
        """Gets the version for a single replica in the greatest contiguous
           prefix."""
        try:
            c = self.v[replica_id].prefix_max
            return Version(replica_id, c)
        except KeyError:
            return Version(replica_id, 0)

    def get_gcp(self):
        """Returns the greatest contiguous prefix of this set of versions.

           The greatest contiguous prefix is the version vector that will
           dominate the greatest number of versions in this set without
           dominating a version that is not in the set.
           """
        result = VersionVector()
        for replica_id, e in self.v.items():
            if e.prefix_max > 0:
                result.update_version(Version(replica_id, e.prefix_max))
        return result

    def dominates_vv(self, vv):
        """Determines if this version set dominates the given version vector.

           A version set (X) dominates a version vector (Y) when the
           greatest contiguous prefix of X dominates Y."""
        assert isinstance(vv, VersionVector)
        return self.get_gcp().dominates(vv)

    def has_version(self, ver):
        """Determines if the given version is contained in this version set."""
        assert isinstance(ver, Version)

        try:
            e = self.v[ver.replica_id]
        except KeyError:
            return (ver.counter == 0)
        if ver.counter <= e.prefix_max:
            return True
        if ver.counter in e.extras:
            return True
        return False

    def insert_version(self, ver):
        """Inserts a single version into the version set."""
        assert isinstance(ver, Version)

        self._get_element(ver.replica_id).insert(ver.counter)

    def merge(self, other):
        """Merges another VersionSet into this one so that this set contains
           the union of all versions in both."""
        assert isinstance(other, VersionSet)

        for replica_id, oe in other.v.items():
            e = self._get_element(replica_id)
            e.update_prefix_upper_bound(oe.prefix_max)
            e.insert_extras(oe.extras)

    def merge_one_version(self, ver):
        """Merges one version into the version set, including all of the
           versions with the same replica ID prior to it."""
        assert isinstance(ver, Version)

        self._get_element(ver.replica_id) \
            .update_prefix_upper_bound(ver.counter)

    def __str__(self):
        node_ids = self.v.keys()
        node_ids.sort()
        result = "[ "
        for nId in node_ids:
            n = self.v[nId]
            result += str(Version(nId, n.prefix_max))
            if len(n.extras) > 0:
                result += "+[%s]" % ",".join([str(i) for i in n.extras])
            result += " "
        result += "]"
        return result

    def _get_element(self, replica_id):
        try:
            e = self.v[replica_id]
        except KeyError:
            e = VersionSetElement()
            self.v[replica_id] = e
        return e


# vim:set ts=4 sw=4 expandtab:

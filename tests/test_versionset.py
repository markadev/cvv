from cvv.vtypes import Version, VersionSet, VersionVector


def test_empty():
    vs = VersionSet()

    assert vs.empty()
    vs.insert_version(Version('AA', 5))
    assert not vs.empty()


def test_has_version():
    vs = VersionSet([Version('AA', 1), Version('AA', 3), Version('BB', 4)])

    # Every set always has version 0
    assert vs.has_version(Version())
    assert vs.has_version(Version('AA', 0))
    assert vs.has_version(Version('BB', 0))
    assert vs.has_version(Version('CC', 0))

    assert vs.has_version(Version('AA', 1))
    assert vs.has_version(Version('AA', 3))
    assert vs.has_version(Version('BB', 4))
    assert not vs.has_version(Version('AA', 2))
    assert not vs.has_version(Version('AA', 4))
    assert not vs.has_version(Version('BB', 1))
    assert not vs.has_version(Version('BB', 2))
    assert not vs.has_version(Version('BB', 3))
    assert not vs.has_version(Version('CC', 1))


def test_get_gcp_when_empty():
    vs = VersionSet()
    assert vs.get_gcp() == VersionVector()


def test_get_version_when_empty():
    vs = VersionSet()
    assert vs.get_version('AA') == Version('AA', 0)
    assert vs.get_version('BB') == Version('BB', 0)


def test_get_gcp_get_version():
    vs = VersionSet()
    vs.insert_version(Version('AA', 1))
    vs.insert_version(Version('AA', 2))
    vs.insert_version(Version('AA', 4))
    vs.insert_version(Version('BB', 1))
    vs.insert_version(Version('BB', 3))
    vs.insert_version(Version('CC', 20))
    expected_gcp = VersionVector()
    expected_gcp.update_version(Version('AA', 2))
    expected_gcp.update_version(Version('BB', 1))
    assert vs.get_gcp() == expected_gcp
    assert vs.get_version('AA') == expected_gcp.get_version('AA')
    assert vs.get_version('BB') == expected_gcp.get_version('BB')
    assert vs.get_version('CC') == expected_gcp.get_version('CC')

    vs.insert_version(Version('BB', 2))
    expected_gcp.update_version(Version('BB', 3))
    assert vs.get_gcp() == expected_gcp
    assert vs.get_version('AA') == expected_gcp.get_version('AA')
    assert vs.get_version('BB') == expected_gcp.get_version('BB')
    assert vs.get_version('CC') == expected_gcp.get_version('CC')

    vs.insert_version(Version('AA', 3))
    expected_gcp.update_version(Version('AA', 4))
    assert vs.get_gcp() == expected_gcp
    assert vs.get_version('AA') == expected_gcp.get_version('AA')
    assert vs.get_version('BB') == expected_gcp.get_version('BB')
    assert vs.get_version('CC') == expected_gcp.get_version('CC')


def test_merge_one_version():
    vs = VersionSet()
    vs.insert_version(Version('AA', 1))
    vs.insert_version(Version('AA', 2))
    vs.insert_version(Version('BB', 5))
    expected_gcp = VersionVector()
    expected_gcp.update_version(Version('AA', 2))
    assert vs.get_gcp() == expected_gcp

    vs.merge_one_version(Version('AA', 6))
    expected_gcp.update_version(Version('AA', 6))
    assert vs.get_gcp() == expected_gcp

    vs.merge_one_version(Version('BB', 10))
    expected_gcp.update_version(Version('BB', 10))
    assert vs.get_gcp() == expected_gcp

    vs.merge_one_version(Version('CC', 8))
    expected_gcp.update_version(Version('CC', 8))
    assert vs.get_gcp() == expected_gcp


# vim:set ts=4 sw=4 expandtab:

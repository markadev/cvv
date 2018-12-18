import pytest

from cvv.vtypes import Version


def test_comparison():
    r1_1 = Version('AA', 1)
    r1_2 = Version('AA', 2)
    r2_1 = Version('BB', 1)
    r2_1p = Version('BB', 1)

    with pytest.raises(TypeError):
        r1_1 < r1_2

    with pytest.raises(TypeError):
        r1_1 > r1_2

    with pytest.raises(TypeError):
        r1_1 <= r1_2

    with pytest.raises(TypeError):
        r1_1 >= r1_2

    assert r1_1 != r1_2
    assert r1_1 != r2_1
    assert r2_1 == r2_1p


def test_str():
    assert str(Version()) == 'None:0'
    assert str(Version('AA', 3)) == 'AA:3'


def test_repr():
    assert repr(Version()) == 'Version(None, 0)'
    assert repr(Version('AA', 3)) == "Version('AA', 3)"


# vim:set ts=4 sw=4 expandtab:

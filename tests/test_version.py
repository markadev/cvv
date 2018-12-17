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


def test_string():
    v1 = Version('AA', 5)
    assert str(v1) == 'AA:5'


# vim:set ts=4 sw=4 expandtab:

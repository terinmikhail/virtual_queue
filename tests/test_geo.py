"""Тесты геометрии: distance_m, within_radius."""
from core.config import GeoPoint
from core.geo import distance_m, within_radius


def test_distance_zero():
    p = GeoPoint(49.1288, 142.6601)
    assert distance_m(p, p) == 0.0


def test_distance_known_1km():
    # 0.009 градуса широты ~ 1000 м (на широте 49)
    a = GeoPoint(49.0, 142.0)
    b = GeoPoint(49.009, 142.0)
    d = distance_m(a, b)
    assert 990 <= d <= 1010   # ~1 км с допуском


def test_distance_symmetry():
    a = GeoPoint(49.0, 142.0)
    b = GeoPoint(49.01, 142.01)
    assert abs(distance_m(a, b) - distance_m(b, a)) < 1e-6


def test_within_radius_inside():
    center = GeoPoint(49.1288, 142.6601)
    p = GeoPoint(49.1288, 142.6601)   # та же точка
    assert within_radius(p, center, 50.0) is True


def test_within_radius_boundary_inclusive():
    # точка ровно на границе — должна считаться «внутри» (<=)
    center = GeoPoint(0.0, 0.0)
    # 50 м по широте ~ 0.000449 градуса
    boundary = GeoPoint(50 / 111_320, 0.0)
    assert within_radius(boundary, center, 50.0) is True


def test_within_radius_outside():
    center = GeoPoint(0.0, 0.0)
    far = GeoPoint(0.001, 0.0)   # ~111 м
    assert within_radius(far, center, 50.0) is False
import math

from core.config import GeoPoint

EARTH_RADIUS_M = 6_371_000.0


def distance_m(a: GeoPoint, b: GeoPoint) -> float:
    """Расстояние по прямой между двумя точками (haversine), метры."""
    lat1, lon1 = math.radians(a.lat), math.radians(a.lon)
    lat2, lon2 = math.radians(b.lat), math.radians(b.lon)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(h))


def within_radius(a: GeoPoint, center: GeoPoint, radius_m: float) -> bool:
    return distance_m(a, center) <= radius_m
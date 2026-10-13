function hasFinitePair(first, second) {
  return typeof first === 'number' && Number.isFinite(first)
    && typeof second === 'number' && Number.isFinite(second);
}

export function summarizeCandidateRepresentation(candidate) {
  const points = (candidate?.groups || []).flatMap(group => group?.points || []);
  return Object.freeze({
    coordinateSystemKind: String(candidate?.coordinateSystem?.kind || 'none'),
    coordinateSystemStatus: String(candidate?.coordinateSystem?.status || 'none'),
    pointCount: points.length,
    geographicPairCount: points.filter(point => hasFinitePair(point?.latitude, point?.longitude)).length,
    projectedPairCount: points.filter(point => hasFinitePair(point?.x, point?.y)).length,
    incompleteGeographicPairCount: points.filter(point => (
      hasFinitePair(point?.latitude, point?.longitude) === false
      && ((typeof point?.latitude === 'number' && Number.isFinite(point.latitude))
        || (typeof point?.longitude === 'number' && Number.isFinite(point.longitude)))
    )).length,
    incompleteProjectedPairCount: points.filter(point => (
      hasFinitePair(point?.x, point?.y) === false
      && ((typeof point?.x === 'number' && Number.isFinite(point.x))
        || (typeof point?.y === 'number' && Number.isFinite(point.y)))
    )).length,
    reviewPointCount: points.filter(point => point?.needsReview === true).length,
  });
}

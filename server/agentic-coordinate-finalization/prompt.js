const OUTPUT_CONTRACT = `Return exactly one JSON object with this shape:
{
  "success": true,
  "resultStatus": "usable | needs_review | failed",
  "displayText": "the current coordinate text, kept easy to compare with the user's text",
  "coordinateSystem": {
    "kind": "geographic | projected | unknown",
    "name": "printed CRS name or null",
    "epsg": "EPSG code or null",
    "status": "identified | needs_confirmation | unknown"
  },
  "geometryType": "Point | MultiPoint | LineString | Polygon | MultiPolygon | Grid | Unknown",
  "groups": [
    {
      "name": "visible group/site name or null",
      "points": [
        {
          "label": "visible point label or null",
          "sourceText": "the exact current-text row",
          "x": null,
          "y": null,
          "latitude": 0,
          "longitude": 0,
          "needsReview": false
        }
      ]
    }
  ],
  "warnings": []
}`;

export function buildAgenticCoordinateFinalizationPrompt({ currentText, recognitionContext }) {
  const context = recognitionContext && typeof recognitionContext === 'object'
    ? JSON.stringify({
      coordinateSystem: recognitionContext.coordinateSystem || null,
      geometryType: recognitionContext.geometryType || null,
      groups: recognitionContext.groups || null,
    })
    : 'null';

  return `You are finalizing the coordinate document currently visible to the user.

The CURRENT TEXT below is the only coordinate content with authority. The earlier recognition context is advisory evidence only. If the user edited a value, the current text wins. Never restore an older value from the context.

Understand the document as a whole. Preserve visible group boundaries; never create groups merely because there are four rows. A list of observation/sample points is MultiPoint, not a polygon. A printed cadastral grid list is Grid, not one boundary polygon. Do not guess a CRS, UTM zone, hemisphere, datum, EPSG code, sign, digit, or missing coordinate.

For DMS or decimal geographic coordinates, normalize longitude/latitude numerically while preserving each current source row in sourceText. For projected coordinates, preserve X/Y and explicit CRS evidence; do not invent latitude/longitude. Mark uncertain rows and the overall result as needs_review. If no reliable coordinate document remains, return failed with empty groups.

${OUTPUT_CONTRACT}

EARLIER RECOGNITION CONTEXT (advisory only):
${context}

CURRENT TEXT (authoritative):
---BEGIN CURRENT COORDINATE TEXT---
${String(currentText || '')}
---END CURRENT COORDINATE TEXT---`;
}


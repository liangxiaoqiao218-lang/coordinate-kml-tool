const DEFAULT_LIMITS = Object.freeze({
  maxBytes: 2 * 1024 * 1024,
  maxDepth: 40,
  maxPoints: 100000,
  maxObjects: 5000
});

export class KmlImportError extends Error {
  constructor(code, message, details = {}) {
    super(message);
    this.name = "KmlImportError";
    this.code = code;
    this.details = details;
  }
}

const localName = name => String(name || "").split(":").pop();
const KML_NAMESPACES = new Set(["", "http://www.opengis.net/kml/2.2", "http://earth.google.com/kml/2.0", "http://earth.google.com/kml/2.1"]);
const KML_CORE_ELEMENTS = new Set(["kml", "Document", "Folder", "Placemark", "Point", "LineString", "Polygon", "MultiGeometry", "outerBoundaryIs", "innerBoundaryIs", "LinearRing", "coordinates", "name", "description", "ExtendedData", "Data", "value", "SchemaData", "SimpleData", "Style", "styleUrl", "StyleMap", "Schema", "visibility", "open", "NetworkLink"]);

function fail(code, message, details) {
  throw new KmlImportError(code, message, details);
}

function decodeXml(value) {
  return String(value).replace(/&([^;]*);|&/g, (entity, key) => {
    if (key === undefined) fail("INVALID_ENTITY", "Unescaped ampersand in XML.");
    if (key === "amp") return "&";
    if (key === "lt") return "<";
    if (key === "gt") return ">";
    if (key === "quot") return "\"";
    if (key === "apos") return "'";
    if (!/^#(?:x[0-9a-f]+|[0-9]+)$/i.test(key)) {
      fail("UNSUPPORTED_ENTITY", "Unsupported XML entity.");
    }
    const point = /^#x/i.test(key) ? Number.parseInt(key.slice(2), 16) : Number.parseInt(key.slice(1), 10);
    const valid = point === 9 || point === 10 || point === 13
      || (point >= 0x20 && point <= 0xd7ff)
      || (point >= 0xe000 && point <= 0xfffd)
      || (point >= 0x10000 && point <= 0x10ffff);
    if (!valid) fail("INVALID_ENTITY", "XML entity contains an invalid code point.");
    return String.fromCodePoint(point);
  });
}

function findTagEnd(xml, start) {
  let quote = null;
  for (let index = start; index < xml.length; index += 1) {
    const char = xml[index];
    if (quote) {
      if (char === quote) quote = null;
    } else if (char === "\"" || char === "'") {
      quote = char;
    } else if (char === ">") {
      return index;
    }
  }
  fail("CORRUPT_XML", "Unterminated XML tag.");
}

function parseAttributes(source) {
  const attributes = Object.create(null);
  let rest = source.trim();
  while (rest) {
    const match = rest.match(/^([^\s=/>]+)\s*=\s*("([^"]*)"|'([^']*)')\s*/);
    if (!match) fail("CORRUPT_XML", `Malformed XML attributes near: ${rest.slice(0, 40)}`);
    if (!/^[A-Za-z_][\w.-]*(?::[A-Za-z_][\w.-]*)?$/.test(match[1])) fail("CORRUPT_XML", "Invalid attribute name.");
    if (Object.hasOwn(attributes, match[1])) fail("CORRUPT_XML", "Duplicate XML attribute.");
    attributes[match[1]] = decodeXml(match[3] ?? match[4] ?? "");
    rest = rest.slice(match[0].length);
  }
  return attributes;
}

function parseXml(xml, limits) {
  if (typeof xml !== "string") fail("INVALID_INPUT", "KML input must be text.");
  if (new TextEncoder().encode(xml).length > limits.maxBytes) {
    fail("SIZE_LIMIT", `KML exceeds ${limits.maxBytes} bytes.`);
  }
  if (/<!\s*(DOCTYPE|ENTITY)\b/i.test(xml)) {
    fail("DTD_FORBIDDEN", "DOCTYPE and entity declarations are forbidden.");
  }

  const documentNode = { name: "#document", attrs: {}, namespaces: {}, children: [], text: "", start: 0, end: xml.length };
  const stack = [documentNode];
  let cursor = 0;
  while (cursor < xml.length) {
    const open = xml.indexOf("<", cursor);
    if (open < 0) {
      stack.at(-1).text += decodeXml(xml.slice(cursor));
      break;
    }
    if (open > cursor) stack.at(-1).text += decodeXml(xml.slice(cursor, open));

    if (xml.startsWith("<!--", open)) {
      const end = xml.indexOf("-->", open + 4);
      if (end < 0) fail("CORRUPT_XML", "Unterminated XML comment.");
      cursor = end + 3;
      continue;
    }
    if (xml.startsWith("<?", open)) {
      const end = xml.indexOf("?>", open + 2);
      if (end < 0) fail("CORRUPT_XML", "Unterminated processing instruction.");
      cursor = end + 2;
      continue;
    }
    if (xml.startsWith("<![CDATA[", open)) {
      const end = xml.indexOf("]]>", open + 9);
      if (end < 0) fail("CORRUPT_XML", "Unterminated CDATA section.");
      stack.at(-1).text += xml.slice(open + 9, end);
      cursor = end + 3;
      continue;
    }
    if (xml.startsWith("<!", open)) fail("FORBIDDEN_DECLARATION", "XML declarations other than comments and CDATA are forbidden.");

    const end = findTagEnd(xml, open + 1);
    const body = xml.slice(open + 1, end).trim();
    if (!body) fail("CORRUPT_XML", "Empty XML tag.");
    if (body.startsWith("/")) {
      const closingName = body.slice(1).trim();
      if (stack.length === 1 || stack.at(-1).name !== closingName) {
        fail("CORRUPT_XML", `Mismatched closing tag ${closingName}.`);
      }
      const node = stack.pop();
      node.end = end + 1;
      node.raw = xml.slice(node.start, node.end);
      cursor = end + 1;
      continue;
    }

    const selfClosing = /\/\s*$/.test(body);
    const cleaned = selfClosing ? body.replace(/\/\s*$/, "").trim() : body;
    const nameMatch = cleaned.match(/^([^\s/>]+)/);
    if (!nameMatch) fail("CORRUPT_XML", "Malformed XML element name.");
    const name = nameMatch[1];
    if (!/^[A-Za-z_][\w.-]*(?::[A-Za-z_][\w.-]*)?$/.test(name)) fail("CORRUPT_XML", "Invalid element name.");
    const node = {
      name,
      attrs: parseAttributes(cleaned.slice(name.length)),
      children: [],
      text: "",
      start: open,
      end: selfClosing ? end + 1 : null,
      raw: selfClosing ? xml.slice(open, end + 1) : null
    };
    node.namespaces = { ...stack.at(-1).namespaces };
    for (const [key, value] of Object.entries(node.attrs)) {
      if (key === "xmlns") node.namespaces[""] = value;
      else if (key.startsWith("xmlns:")) node.namespaces[key.slice(6)] = value;
    }
    const prefix = name.includes(":") ? name.split(":")[0] : "";
    if (prefix && !node.namespaces[prefix]) fail("UNBOUND_PREFIX", "Element prefix is not declared.");
    node.namespaceURI = node.namespaces[prefix] || "";
    if (KML_CORE_ELEMENTS.has(localName(name)) && !KML_NAMESPACES.has(node.namespaceURI)) {
      fail("UNSUPPORTED_KML_NAMESPACE", "Recognized KML element uses a foreign namespace.");
    }
    stack.at(-1).children.push(node);
    if (!selfClosing) {
      stack.push(node);
      if (stack.length - 1 > limits.maxDepth) fail("DEPTH_LIMIT", `XML nesting exceeds ${limits.maxDepth}.`);
    }
    cursor = end + 1;
  }
  if (stack.length !== 1) fail("CORRUPT_XML", `Unclosed XML element ${stack.at(-1).name}.`);
  if (documentNode.text.trim()) fail("CORRUPT_XML", "Text outside the root element is forbidden.");
  const roots = documentNode.children.filter(child => child.name !== "#text");
  if (roots.length !== 1 || localName(roots[0].name) !== "kml") fail("NOT_KML", "Document root must be <kml>.");
  return roots[0];
}

const childrenNamed = (node, name) => node.children.filter(child => localName(child.name) === name);
const childNamed = (node, name) => childrenNamed(node, name)[0] || null;

function textContent(node) {
  if (!node) return "";
  return node.text + node.children.map(textContent).join("");
}

function parseCoordinates(node, context) {
  if (!node) fail("MISSING_COORDINATES", "Geometry is missing <coordinates>.");
  const rawText = textContent(node);
  const tokens = rawText.trim().split(/\s+/).filter(Boolean);
  if (!tokens.length) fail("MISSING_COORDINATES", "Coordinate list is empty.");
  const positions = tokens.map((token, index) => {
    const parts = token.split(",");
    if (parts.length < 2 || parts.length > 3 || parts.some(part => part.trim() === "")) {
      fail("INVALID_COORDINATE", `Coordinate ${index + 1} must be lon,lat[,alt].`);
    }
    const numeric = parts.map(Number);
    if (!numeric.every(Number.isFinite)) fail("NON_FINITE_COORDINATE", `Coordinate ${index + 1} is not finite.`);
    if (!parts.every(part => /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/.test(part))) {
      fail("INVALID_COORDINATE_NUMBER", "KML coordinate numbers must be decimal numeric literals.");
    }
    const [lon, lat, alt] = numeric;
    if (lon < -180 || lon > 180 || lat < -90 || lat > 90) {
      fail("OUT_OF_RANGE_COORDINATE", `Coordinate ${index + 1} is outside WGS84 longitude/latitude bounds.`);
    }
    context.pointCount += 1;
    if (context.pointCount > context.limits.maxPoints) fail("POINT_LIMIT", `KML exceeds ${context.limits.maxPoints} points.`);
    return { lon, lat, ...(parts.length === 3 ? { alt } : {}), rawTuple: token };
  });
  return { rawText, coordinateDisplayText: rawText.trim(), positions };
}

function samePosition(a, b) {
  return a.lon === b.lon && a.lat === b.lat && (a.alt ?? null) === (b.alt ?? null);
}

function parseLinearRing(ringNode, context) {
  if (childrenNamed(ringNode, "coordinates").length !== 1) fail("GEOMETRY_CARDINALITY", "LinearRing must contain exactly one coordinate element.");
  const coordinates = parseCoordinates(childNamed(ringNode, "coordinates"), context);
  if (coordinates.positions.length < 4 || !samePosition(coordinates.positions[0], coordinates.positions.at(-1))) {
    fail("UNCLOSED_RING", "Polygon rings must contain at least four positions and be explicitly closed.");
  }
  return coordinates;
}

function parseGeometry(node, context) {
  const type = localName(node.name);
  if ((type === "Point" || type === "LineString") && childrenNamed(node, "coordinates").length !== 1) {
    fail("GEOMETRY_CARDINALITY", "Geometry must contain exactly one coordinate element.");
  }
  if (type === "Point") {
    const coordinates = parseCoordinates(childNamed(node, "coordinates"), context);
    if (coordinates.positions.length !== 1) fail("INVALID_POINT", "Point must contain exactly one coordinate.");
    return { type, ...coordinates, rawSource: node.raw };
  }
  if (type === "LineString") {
    const coordinates = parseCoordinates(childNamed(node, "coordinates"), context);
    if (coordinates.positions.length < 2) fail("INVALID_LINESTRING", "LineString must contain at least two coordinates.");
    return { type, ...coordinates, rawSource: node.raw };
  }
  if (type === "Polygon") {
    const outerNodes = childrenNamed(node, "outerBoundaryIs");
    if (outerNodes.length !== 1) fail("INVALID_POLYGON", "Polygon must contain exactly one outer boundary.");
    const outerRingNode = childNamed(outerNodes[0], "LinearRing");
    if (!outerRingNode) fail("INVALID_POLYGON", "Outer boundary must contain LinearRing.");
    const outer = parseLinearRing(outerRingNode, context);
    const inner = childrenNamed(node, "innerBoundaryIs").map(boundary => {
      const ring = childNamed(boundary, "LinearRing");
      if (!ring) fail("INVALID_POLYGON", "Inner boundary must contain LinearRing.");
      return parseLinearRing(ring, context);
    });
    return { type, outer, inner, rawSource: node.raw };
  }
  if (type === "MultiGeometry") {
    if (node.children.some(child => !["Point", "LineString", "Polygon", "MultiGeometry"].includes(localName(child.name)))) {
      fail("UNSUPPORTED_GEOMETRY", "MultiGeometry includes an unsupported geometry; nothing was silently omitted.");
    }
    const geometryNodes = node.children.filter(child => ["Point", "LineString", "Polygon", "MultiGeometry"].includes(localName(child.name)));
    if (!geometryNodes.length) fail("INVALID_MULTIGEOMETRY", "MultiGeometry must contain at least one supported geometry.");
    return { type, geometries: geometryNodes.map(child => parseGeometry(child, context)), rawSource: node.raw };
  }
  fail("UNSUPPORTED_GEOMETRY", `Unsupported KML geometry ${type}.`);
}

function parseExtendedData(node) {
  if (!node) return null;
  const values = [];
  for (const child of node.children) {
    const type = localName(child.name);
    if (type === "Data") {
      values.push({ kind: "Data", name: child.attrs.name ?? "", value: textContent(childNamed(child, "value")) });
    } else if (type === "SimpleData") {
      values.push({ kind: "SimpleData", name: child.attrs.name ?? "", value: textContent(child) });
    } else {
      values.push({ kind: "Unsupported", element: type, rawSource: child.raw });
    }
  }
  return { values, rawSource: node.raw };
}

function parsePlacemark(node, groupPath, context, order) {
  context.objectCount += 1;
  if (context.objectCount > context.limits.maxObjects) fail("OBJECT_LIMIT", `KML exceeds ${context.limits.maxObjects} objects.`);
  if (node.children.some(child => ["Model", "Track", "MultiTrack", "LinearRing"].includes(localName(child.name)))) {
    fail("UNSUPPORTED_GEOMETRY", "Placemark includes an unsupported geometry; nothing was silently omitted.");
  }
  const geometryNodes = node.children.filter(child => ["Point", "LineString", "Polygon", "MultiGeometry"].includes(localName(child.name)));
  if (geometryNodes.length !== 1) fail("GEOMETRY_CARDINALITY", "Placemark must contain exactly one supported geometry root.");
  const unsupported = node.children
    .map(child => localName(child.name))
    .filter(name => !["name", "description", "ExtendedData", "Point", "LineString", "Polygon", "MultiGeometry", "styleUrl", "Style", "visibility", "open"].includes(name));
  return {
    id: `kml-object-${context.objectCount}`,
    order,
    groupPath: groupPath.map(group => ({ id: group.id, name: group.name })),
    name: textContent(childNamed(node, "name")),
    description: textContent(childNamed(node, "description")),
    descriptionHandling: "PLAIN_TEXT_ONLY_DO_NOT_EXECUTE",
    extendedData: parseExtendedData(childNamed(node, "ExtendedData")),
    originalGeometryType: localName(geometryNodes[0].name),
    geometry: parseGeometry(geometryNodes[0], context),
    rawSource: node.raw,
    warnings: [
      ...unsupported.map(name => `UNSUPPORTED_PLACEMARK_ELEMENT_PRESERVED_IN_RAW_SOURCE:${name}`),
      ...(childNamed(node, "Style") || childNamed(node, "styleUrl") ? ["STYLE_PRESERVED_IN_RAW_SOURCE_NOT_APPLIED"] : [])
    ]
  };
}

function importContainer(node, groupPath, context, structureParent) {
  let order = 0;
  for (const child of node.children) {
    const type = localName(child.name);
    if (type === "Folder" || type === "Document") {
      const group = {
        id: `kml-group-${++context.groupCount}`,
        type,
        name: textContent(childNamed(child, "name")),
        order: order++,
        rawSource: child.raw,
        warnings: [],
        children: []
      };
      context.groups.push(group);
      structureParent.children.push({ kind: "group", id: group.id, children: group.children });
      importContainer(child, [...groupPath, group], context, group);
    } else if (type === "Placemark") {
      const object = parsePlacemark(child, groupPath, context, order++);
      context.objects.push(object);
      structureParent.children.push({ kind: "object", id: object.id });
    } else if (["name", "description", "open", "visibility", "Style", "StyleMap", "Schema"].includes(type)) {
      context.documentWarnings.add(`${type.toUpperCase()}_PRESERVED_IN_RAW_SOURCE_NOT_APPLIED`);
    } else if (type === "NetworkLink") {
      fail("NETWORK_LINK_FORBIDDEN", "NetworkLink is not permitted in local import.");
    } else if (type) {
      context.documentWarnings.add(`UNSUPPORTED_DOCUMENT_ELEMENT_PRESERVED_IN_RAW_SOURCE:${type}`);
    }
  }
}

function findForbiddenExternalElements(node) {
  const type = localName(node.name);
  if (type === "NetworkLink") fail("NETWORK_LINK_FORBIDDEN", "NetworkLink is not permitted in local import.");
  for (const child of node.children) findForbiddenExternalElements(child);
}

export function importKmlText(xml, options = {}) {
  const limits = { ...DEFAULT_LIMITS, ...(options.limits || {}) };
  const root = parseXml(xml, limits);
  findForbiddenExternalElements(root);
  const context = {
    limits,
    pointCount: 0,
    objectCount: 0,
    groupCount: 0,
    objects: [],
    groups: [],
    documentWarnings: new Set()
  };
  const structure = { kind: "root", children: [] };
  importContainer(root, [], context, structure);
  if (!context.objects.length) fail("NO_GEOMETRY_OBJECTS", "KML contains no supported Placemark geometry.");
  return {
    contract: "geokit_local_kml_import_v1",
    sourceFormat: "KML",
    sourceAxisOrder: "LON_LAT_EXPLICIT_KML",
    sourceCRS: "OGC_KML_WGS84",
    editState: "CLEAN",
    requiresReparse: false,
    mapEligible: true,
    kmlEligible: true,
    rawSource: xml,
    rawSourcePreserved: true,
    objects: context.objects,
    groups: context.groups,
    structure,
    warnings: [...context.documentWarnings],
    statistics: { objects: context.objectCount, groups: context.groupCount, points: context.pointCount }
  };
}

export function importKmlFile({ name, text }, options = {}) {
  if (/\.kmz$/i.test(String(name || ""))) fail("KMZ_UNSUPPORTED", "KMZ is not supported; provide plain KML text.");
  if (name && !/\.kml$/i.test(String(name))) fail("FILE_TYPE_UNSUPPORTED", "Only .kml files are supported.");
  return importKmlText(text, options);
}

export function tryImportKmlText(xml, previousState = null, options = {}) {
  try {
    return { ok: true, state: importKmlText(xml, options), error: null };
  } catch (error) {
    return {
      ok: false,
      state: previousState,
      error: {
        code: error instanceof KmlImportError ? error.code : "UNEXPECTED_IMPORT_ERROR",
        message: error instanceof Error ? error.message : "Unknown KML import error."
      }
    };
  }
}

function geometryToGeoJson(geometry) {
  if (geometry.type === "Point") return { type: "Point", coordinates: positionToArray(geometry.positions[0]) };
  if (geometry.type === "LineString") return { type: "LineString", coordinates: geometry.positions.map(positionToArray) };
  if (geometry.type === "Polygon") {
    return { type: "Polygon", coordinates: [geometry.outer.positions, ...geometry.inner.map(ring => ring.positions)].map(ring => ring.map(positionToArray)) };
  }
  if (geometry.type === "MultiGeometry") return { type: "GeometryCollection", geometries: geometry.geometries.map(geometryToGeoJson) };
  fail("UNSUPPORTED_GEOMETRY", `Cannot adapt ${geometry.type} to GeoJSON.`);
}

function positionToArray(position) {
  return Object.hasOwn(position, "alt") ? [position.lon, position.lat, position.alt] : [position.lon, position.lat];
}

function assertConsumable(model) {
  if (!model || model.editState !== "CLEAN" || model.requiresReparse || !model.mapEligible || !model.kmlEligible) {
    fail("DIRTY_MODEL_BLOCKED", "Edited KML text must be parsed again before map or KML consumption.");
  }
}

export function toGeoJsonFeatureCollection(model) {
  assertConsumable(model);
  return {
    type: "FeatureCollection",
    features: model.objects.map(object => ({
      type: "Feature",
      id: object.id,
      properties: {
        name: object.name,
        description: object.description,
        descriptionHandling: object.descriptionHandling,
        originalGeometryType: object.originalGeometryType,
        groupPath: object.groupPath,
        order: object.order,
        extendedData: object.extendedData?.values ?? [],
        warnings: object.warnings
      },
      geometry: geometryToGeoJson(object.geometry)
    }))
  };
}

// An unchanged imported document is exported verbatim, including metadata,
// styles, unknown non-geometry elements, raw tuple formatting and group order.
// These external references are retained as source only; this module never fetches.
export function exportKml(model) {
  assertConsumable(model);
  if (typeof model.rawSource !== "string") fail("INVALID_MODEL", "Original KML source is required for lossless export.");
  return model.rawSource;
}

export function markKmlSourceTextDirty(model, editedText) {
  if (!model) fail("INVALID_MODEL", "A parsed model is required.");
  return {
    ...model,
    editState: "DIRTY_SOURCE_TEXT",
    requiresReparse: true,
    mapEligible: false,
    kmlEligible: false,
    rawSource: editedText,
    objects: model.objects.map(object => ({
      ...object,
      geometry: null,
      warnings: [...object.warnings, "GEOMETRY_CLEARED_AFTER_SOURCE_EDIT"]
    }))
  };
}

export const KML_IMPORT_LIMITS = DEFAULT_LIMITS;


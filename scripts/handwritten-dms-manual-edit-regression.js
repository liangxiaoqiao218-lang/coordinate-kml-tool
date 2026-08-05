const DEGREE_SYMBOL = String.fromCharCode(176);

function decimalFromDms(degrees, minutes, seconds, direction) {
  const deg = Number(degrees);
  const min = Number(minutes);
  const sec = Number(seconds);

  if (!Number.isFinite(deg) || !Number.isFinite(min) || !Number.isFinite(sec)) {
    return null;
  }

  let value = Math.abs(deg) + min / 60 + sec / 3600;
  const dir = String(direction || "").toUpperCase();

  if (["S", "W", "O"].includes(dir) || (!dir && deg < 0)) {
    value = -value;
  }

  return value;
}

function parseHandwrittenDmsPart(part) {
  const normalized = String(part || "")
    .toUpperCase()
    .replace(/[\u00BA\u02DA]/g, DEGREE_SYMBOL)
    .replace(/[\u2018\u2019\u00B4`\u2032]/g, "'")
    .replace(/[\u201C\u201D\u2033]/g, '"')
    .trim();
  const direction = (normalized.match(/[NSEWO]/)?.[0] || "").toUpperCase();
  const body = normalized.replace(/[NSEWO]/g, " ").trim();
  let pieces = [];

  const dotDms = body.match(/^([-+]?\d{1,3})\.(\d{1,2})\.(\d{1,2})(?:\.(\d+))?$/);
  if (dotDms) {
    pieces = [
      dotDms[1],
      dotDms[2],
      dotDms[4] ? `${dotDms[3]}.${dotDms[4]}` : dotDms[3]
    ];
  } else if (body.includes(DEGREE_SYMBOL)) {
    const [degreeText, restText = ""] = body.split(DEGREE_SYMBOL);
    const degrees = degreeText.match(/[-+]?\d+/)?.[0];
    const groups = restText.trim().match(/\d+(?:\.\d+)?/g) || [];

    if (groups.length === 2 && groups[0].includes(".")) {
      const [minutes, secondsStart] = groups[0].split(".");
      pieces = [degrees, minutes, `${secondsStart}.${groups[1].replace(/^0\./, "")}`];
    } else if (groups.length >= 3) {
      pieces = [degrees, groups[0], `${groups[1]}.${groups.slice(2).join("")}`];
    } else {
      pieces = [degrees, ...groups];
    }
  } else {
    const groups = body.match(/\d+(?:\.\d+)?/g) || [];
    if (groups.length >= 4) {
      pieces = [groups[0], groups[1], `${groups[2]}.${groups.slice(3).join("")}`];
    } else {
      pieces = groups;
    }
  }

  if (pieces.length < 3) {
    return null;
  }

  return {
    axis: ["N", "S"].includes(direction) ? "lat" : "lon",
    value: decimalFromDms(pieces[0], pieces[1], pieces[2], direction)
  };
}

function parseHandwrittenDmsLine(line) {
  const partPattern = /[-+]?\d{1,3}(?:(?:\s*\u00B0\s*|\s+)\d{1,2}(?:[\s.'"\u2032]+\d{1,2}){1,2}(?:\.\d+)?|\.\d{1,2}\.\d{1,2}(?:\.\d+)?)\s*["\u2033]?\s*[NSEWO]/gi;
  const parts = String(line || "").match(partPattern) || [];
  const parsed = parts.map(parseHandwrittenDmsPart).filter(Boolean);
  const lat = parsed.find(item => item.axis === "lat");
  const lon = parsed.find(item => item.axis === "lon");

  if (!lat || !lon) {
    return null;
  }

  return { latitude: lat.value, longitude: lon.value };
}

function assertClose(name, actual, expected, tolerance = 1e-9) {
  if (!Number.isFinite(actual) || Math.abs(actual - expected) > tolerance) {
    throw new Error(`${name}: expected ${expected}, actual ${actual}`);
  }
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

const goldenCases = [
  {
    name: "HANDWRITTEN_DMS_MANUAL_EDIT_CASE dot separators",
    input: "11\u00B028.31.26N 08.40.42.13W",
    latitude: 11.47535,
    longitude: -8.678369444444444
  },
  {
    name: "HANDWRITTEN_DMS_MANUAL_EDIT_CASE missing quotes",
    input: "11\u00B027'45.54N 08\u00B036'08.06W",
    latitude: 11.46265,
    longitude: -8.602238888888888
  },
  {
    name: "HANDWRITTEN_DMS_SPACE_SEPARATOR_CASE split seconds",
    input: "11\u00B027 45 09 N 08 36 30.76 W",
    latitude: 11.462525,
    longitude: -8.608544444444444
  },
  {
    name: "HANDWRITTEN_DMS_SPACE_SEPARATOR_CASE decimal tail",
    input: "11\u00B027 45.09 N 08\u00B036 30.76 W",
    latitude: 11.462525,
    longitude: -8.608544444444444
  }
];

for (const testCase of goldenCases) {
  const parsed = parseHandwrittenDmsLine(testCase.input);
  assert(parsed, `${testCase.name}: parser returned null`);
  assertClose(`${testCase.name} latitude`, parsed.latitude, testCase.latitude);
  assertClose(`${testCase.name} longitude`, parsed.longitude, testCase.longitude);
}

console.log("HANDWRITTEN_DMS_MANUAL_EDIT_CASE PASS");

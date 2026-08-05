const DMS_TOKEN_PATTERN = /([-+]?\d{1,3})\s*[°º]\s*(\d{1,2})\s*(?:['′’]|\.)\s*(\d{1,2}(?:[.,]\d+)?)\s*(?:["″”])?\s*([NSEWO])/gi;

function normalizeDecimalToken(value) {
  return String(value || "").replace(",", ".");
}

export function dmsToDecimal({ degrees, minutes, seconds, direction } = {}) {
  const degreeValue = Math.abs(Number(degrees));
  const minuteValue = Number(minutes);
  const secondValue = Number(seconds);
  const normalizedDirection = String(direction || "").toUpperCase() === "O"
    ? "W"
    : String(direction || "").toUpperCase();

  if (!Number.isFinite(degreeValue)
    || !Number.isFinite(minuteValue)
    || !Number.isFinite(secondValue)
    || minuteValue < 0
    || minuteValue >= 60
    || secondValue < 0
    || secondValue >= 60
    || !/[NSEW]/.test(normalizedDirection)) {
    return null;
  }

  const limit = /[NS]/.test(normalizedDirection) ? 90 : 180;
  if (degreeValue > limit) {
    return null;
  }

  const sign = /[SW]/.test(normalizedDirection) ? -1 : 1;
  return sign * (degreeValue + (minuteValue / 60) + (secondValue / 3600));
}

export function parseDmsTokens(text = "") {
  const tokens = [];
  const source = String(text || "");
  const pattern = new RegExp(DMS_TOKEN_PATTERN.source, DMS_TOKEN_PATTERN.flags);
  let match;

  while ((match = pattern.exec(source)) !== null) {
    const direction = match[4].toUpperCase() === "O" ? "W" : match[4].toUpperCase();
    const token = {
      raw: match[0],
      degrees: String(Math.abs(Number(match[1]))),
      minutes: String(Number(match[2])),
      seconds: normalizeDecimalToken(match[3]),
      direction,
      field: /[NS]/.test(direction) ? "latitude" : "longitude"
    };
    token.decimal = dmsToDecimal(token);
    token.valid = token.decimal !== null;
    tokens.push(token);
  }

  return tokens;
}

export function parseDmsRows(text = "") {
  return String(text || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .map((line, index) => {
      const tokens = parseDmsTokens(line);
      if (tokens.length < 1) {
        return null;
      }

      const labelMatch = line.match(/^\s*(?:point\s*)?([A-Z]|\d{1,3})\s*[.):-]?\s+/i);
      const fields = {};
      tokens.forEach(token => {
        if (!fields[token.field]) {
          fields[token.field] = token;
        }
      });

      return {
        label: labelMatch?.[1] || String(index + 1),
        line,
        fields,
        tokens
      };
    })
    .filter(Boolean);
}

export function getDmsPointCoordinates(text = "") {
  const row = parseDmsRows(text)[0];
  const latitude = row?.fields?.latitude?.decimal;
  const longitude = row?.fields?.longitude?.decimal;

  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    return null;
  }

  return { lat: latitude, lon: longitude };
}


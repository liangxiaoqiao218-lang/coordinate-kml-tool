const MAP_TICK_VALUES = new Set([
  "290625",
  "295625",
  "300625",
  "535625",
  "540625",
  "545625",
  "550625"
]);

function normalizeText(text) {
  return String(text || "")
    .replace(/\r\n/g, "\n")
    .replace(/\u00a0/g, " ")
    .replace(/[，]/g, ",")
    .replace(/[｜]/g, "|");
}

export function hasMadagascarCadastralStructuralSignature(text) {
  const value = normalizeText(text);
  const hasListeCarres = /liste[_\s-]*carr[eé]s?/i.test(value);
  const hasXv = /\bX\s*V\b|\bXV\b/i.test(value);
  const hasYv = /\bY\s*V\b|\bYV\b/i.test(value);
  const hasEnhancer = /\bNC\b|\bnum\b|n[°o]\b|CM[_\s-]*NOMFIR|cadastral|cadastre|grille|carreau/i.test(value);

  return Boolean(hasListeCarres && hasXv && hasYv && hasEnhancer);
}

export function hasCadastralGridContext(text) {
  const value = normalizeText(text);
  return hasMadagascarCadastralStructuralSignature(value)
    || (/\bXV\b/i.test(value)
      && /\bYV\b/i.test(value)
      && (/\bnum\b|n[°o]\b|cadastral|cadastre|grid|grille|quadrillage|carreau|矿权|网格/i.test(value)));
}

export function normalizeGridValue(value) {
  return String(value || "")
    .trim()
    .replace(/,/g, ".")
    .replace(/\s+/g, "");
}

function normalizeCellId(value) {
  return String(value || "")
    .trim()
    .replace(/[|,;:]/g, "")
    .replace(/\s+/g, "");
}

function numericToken(value) {
  const normalized = normalizeGridValue(value);
  const number = Number(normalized);
  return Number.isFinite(number) ? number : null;
}

function isMapTickValue(value) {
  return MAP_TICK_VALUES.has(normalizeGridValue(value));
}

function isValidCellNum(value) {
  const normalized = normalizeCellId(value);
  if (!normalized) return false;
  if (/^\d{6,}$/.test(normalized)) return false;
  return /^[A-Za-z0-9-]{1,16}$/.test(normalized);
}

function isValidGridCoordinate(value) {
  const number = numericToken(value);
  return Number.isFinite(number) && number > 0 && number < 1000000;
}

function isValidCadastralGridRow(row) {
  if (!row || !isValidCellNum(row.num) || !isValidGridCoordinate(row.xv) || !isValidGridCoordinate(row.yv)) {
    return false;
  }
  if (isMapTickValue(row.num)) {
    return false;
  }
  return true;
}

function isLikelyNcXvYvNameNumRow(tokens) {
  if (tokens.length < 4) return false;
  const nc = Number(normalizeGridValue(tokens[0]));
  const xv = numericToken(tokens[1]);
  const yv = numericToken(tokens[2]);
  const num = normalizeCellId(tokens[tokens.length - 1]);

  return Number.isInteger(nc)
    && nc >= 1
    && nc <= 999
    && Number.isFinite(xv)
    && Number.isFinite(yv)
    && isValidCellNum(num);
}

export function extractCadastralGridRows(text) {
  // Stable path: Madagascar cadastral grid extraction returns only num | XV | YV.
  // Do not include NC/CM_NOMFIR here and do not convert XV/YV in the backend recognition response.
  if (!hasCadastralGridContext(text)) {
    return [];
  }

  const rows = [];
  const seen = new Set();

  normalizeText(text)
    .split(/\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .forEach(line => {
      if (/^(?:NC\s*)?(?:num|n[°o]?|#)?\s*[\|,;\s-]*x\s*v[\|,;\s-]*y\s*v(?:[\|,;\s-]*(?:CM[_\s-]*NOMFIR|num))?$/i.test(line)
        || /^(?:NC|#)\s*[\|,;\s-]*X\s*V\s*[\|,;\s-]*Y\s*V/i.test(line)) {
        return;
      }

      let row = null;
      const pipeParts = line.split("|").map(part => part.trim()).filter(Boolean);
      if (pipeParts.length >= 3 && !/^NC$/i.test(pipeParts[0]) && !/^num$/i.test(pipeParts[0])) {
        if (pipeParts.length >= 5 && /^[-+]?\d+$/.test(pipeParts[0]) && isValidGridCoordinate(pipeParts[1]) && isValidGridCoordinate(pipeParts[2])) {
          row = {
            num: normalizeCellId(pipeParts[pipeParts.length - 1]),
            xv: normalizeGridValue(pipeParts[1]),
            yv: normalizeGridValue(pipeParts[2])
          };
        } else {
          row = {
            num: normalizeCellId(pipeParts[0]),
            xv: normalizeGridValue(pipeParts[1]),
            yv: normalizeGridValue(pipeParts[2])
          };
        }
      }

      const labeled = line.match(/(?:^|\b)(?:num|n[°o]?|#)?\s*([A-Za-z0-9-]{1,16})\D+XV\D*([-+]?\d+(?:[.,]\d+)?)\D+YV\D*([-+]?\d+(?:[.,]\d+)?)/i);
      if (!row && labeled) {
        row = {
          num: normalizeCellId(labeled[1]),
          xv: normalizeGridValue(labeled[2]),
          yv: normalizeGridValue(labeled[3])
        };
      }

      if (!row) {
        const numericTokens = line.match(/[-+]?\d+(?:[.,]\d+)?/g) || [];
        if (isLikelyNcXvYvNameNumRow(numericTokens)) {
          row = {
            num: normalizeCellId(numericTokens[numericTokens.length - 1]),
            xv: normalizeGridValue(numericTokens[1]),
            yv: normalizeGridValue(numericTokens[2])
          };
        }
      }

      if (!row) {
        const cleaned = line
          .replace(/\b(?:num|n[°o]?|xv|yv)\b/gi, " ")
          .replace(/[|:;，,]/g, " ");
        const tokens = cleaned.match(/[-+]?\d+(?:[.,]\d+)?|[A-Za-z]?\d[A-Za-z0-9-]*/g) || [];

        if (tokens.length >= 3) {
          row = {
            num: normalizeCellId(tokens[0]),
            xv: normalizeGridValue(tokens[1]),
            yv: normalizeGridValue(tokens[2])
          };
        }
      }

      if (!isValidCadastralGridRow(row)) {
        return;
      }

      const key = `${row.num}|${row.xv}|${row.yv}`;
      if (seen.has(key)) {
        return;
      }

      seen.add(key);
      rows.push(row);
    });

  return rows;
}

export function formatCadastralGridRows(rows) {
  return ["num | XV | YV", ...rows.map(row => `${row.num} | ${row.xv} | ${row.yv}`)].join("\n");
}

export function getCadastralGridInfo(text) {
  const rows = extractCadastralGridRows(text);
  return {
    isCadastralGrid: rows.length > 0,
    rows,
    rowCount: rows.length
  };
}

export function hasMapGridTickTakeover(text) {
  const value = normalizeText(text);
  const hits = [...MAP_TICK_VALUES].filter(tick => new RegExp(`\\b${tick}\\b`).test(value));
  return hits.length >= 2 && !hasMadagascarCadastralStructuralSignature(value);
}

export function shouldRunEarlyMadagascarCadastralPriority({
  rawText = "",
  coordinates = "",
  fileName = "",
  rawHint = "",
  hint = ""
} = {}) {
  const combined = [rawText, coordinates, fileName, rawHint, hint].join("\n");
  const structuralSignature = hasMadagascarCadastralStructuralSignature(combined);
  const madagascarCue = /madagascar|madagasikara|马达加斯加|ampasimamitaka|ilakaka|andriandampy/i.test(combined);
  const mapTickTakeover = hasMapGridTickTakeover([rawText, coordinates].join("\n"));
  const candidate = Boolean(structuralSignature || madagascarCue);

  return {
    candidate,
    structuralSignature,
    madagascarCue,
    mapTickTakeover,
    reason: structuralSignature
      ? "liste_carres_xv_yv_signature"
      : madagascarCue && mapTickTakeover
        ? "madagascar_cue_with_map_tick_takeover"
        : madagascarCue
          ? "madagascar_cue_requires_table_focused_acquisition"
        : "no_madagascar_cadastral_signature"
  };
}

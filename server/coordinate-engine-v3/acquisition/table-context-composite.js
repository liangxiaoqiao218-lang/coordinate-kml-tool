import sharp from "sharp";

export const TABLE_CONTEXT_COMPOSITE_MODE = "table_context_composite_experiment";

export const TABLE_CONTEXT_COMPOSITE_STATUS = Object.freeze({
  CREATED: "COMPOSITE_CREATED",
  NO_STRONG_TABLE_REGION: "PREPROCESSING_NO_STRONG_TABLE_REGION",
  ERROR: "PREPROCESSING_ERROR",
});

const ANALYSIS_WIDTH = 400;
const LIGHT_THRESHOLD = 205;
const DARK_THRESHOLD = 96;

function now(clock = Date.now) {
  return Number(clock());
}

function cleanMimeType(value = "") {
  const text = String(value || "").trim().toLowerCase();
  return text.startsWith("image/") ? text : "image/jpeg";
}

function decodeImageBase64(value = "") {
  const text = String(value || "").trim();
  if (!text) return Buffer.alloc(0);
  const payload = text.includes(",") ? text.split(",").pop() : text;
  return Buffer.from(payload, "base64");
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function luma(r, g, b) {
  return (0.299 * r) + (0.587 * g) + (0.114 * b);
}

function isLightPanelPixel(r, g, b) {
  const brightness = luma(r, g, b);
  const yellowish = r > 185 && g > 155 && b < 130;
  return brightness >= LIGHT_THRESHOLD || yellowish;
}

function makeMask(raw, width, height) {
  const mask = new Uint8Array(width * height);
  for (let index = 0; index < width * height; index += 1) {
    const offset = index * 4;
    if (isLightPanelPixel(raw[offset], raw[offset + 1], raw[offset + 2])) {
      mask[index] = 1;
    }
  }
  return mask;
}

function dilateMask(mask, width, height, radius = 2) {
  const output = new Uint8Array(mask.length);
  for (let y = 0; y < height; y += 1) {
    for (let x = 0; x < width; x += 1) {
      const index = (y * width) + x;
      if (mask[index] !== 1) continue;
      for (let dy = -radius; dy <= radius; dy += 1) {
        for (let dx = -radius; dx <= radius; dx += 1) {
          const nx = x + dx;
          const ny = y + dy;
          if (nx >= 0 && nx < width && ny >= 0 && ny < height) {
            output[(ny * width) + nx] = 1;
          }
        }
      }
    }
  }
  return output;
}

function findComponents(mask, width, height) {
  const visited = new Uint8Array(mask.length);
  const components = [];
  const stack = [];
  for (let start = 0; start < mask.length; start += 1) {
    if (mask[start] !== 1 || visited[start] === 1) continue;
    let minX = width;
    let minY = height;
    let maxX = 0;
    let maxY = 0;
    let area = 0;
    stack.push(start);
    visited[start] = 1;
    while (stack.length) {
      const index = stack.pop();
      const x = index % width;
      const y = Math.floor(index / width);
      area += 1;
      minX = Math.min(minX, x);
      minY = Math.min(minY, y);
      maxX = Math.max(maxX, x);
      maxY = Math.max(maxY, y);
      const neighbors = [index - 1, index + 1, index - width, index + width];
      for (const next of neighbors) {
        if (next < 0 || next >= mask.length || visited[next] === 1 || mask[next] !== 1) continue;
        const nx = next % width;
        if (Math.abs(nx - x) > 1) continue;
        visited[next] = 1;
        stack.push(next);
      }
    }
    components.push(Object.freeze({
      x: minX,
      y: minY,
      width: maxX - minX + 1,
      height: maxY - minY + 1,
      area,
    }));
  }
  return components;
}

function countLineSignals(raw, imageWidth, region) {
  const rowSignals = [];
  const columnSignals = [];
  for (let y = region.y; y < region.y + region.height; y += 1) {
    let dark = 0;
    for (let x = region.x; x < region.x + region.width; x += 1) {
      const offset = ((y * imageWidth) + x) * 4;
      if (luma(raw[offset], raw[offset + 1], raw[offset + 2]) < DARK_THRESHOLD) dark += 1;
    }
    if (dark / Math.max(1, region.width) > 0.18) rowSignals.push(y);
  }
  for (let x = region.x; x < region.x + region.width; x += 1) {
    let dark = 0;
    for (let y = region.y; y < region.y + region.height; y += 1) {
      const offset = ((y * imageWidth) + x) * 4;
      if (luma(raw[offset], raw[offset + 1], raw[offset + 2]) < DARK_THRESHOLD) dark += 1;
    }
    if (dark / Math.max(1, region.height) > 0.14) columnSignals.push(x);
  }
  return Object.freeze({
    horizontalLineSignals: compressSignals(rowSignals),
    verticalLineSignals: compressSignals(columnSignals),
  });
}

function compressSignals(values = []) {
  if (!values.length) return 0;
  let count = 1;
  let previous = values[0];
  for (const value of values.slice(1)) {
    if (value - previous > 2) count += 1;
    previous = value;
  }
  return count;
}

function groupSignals(values = [], gap = 3) {
  if (!values.length) return [];
  const groups = [];
  let start = values[0];
  let end = values[0];
  for (const value of values.slice(1)) {
    if (value - end <= gap) {
      end = value;
    } else {
      groups.push(Object.freeze({ start, end, count: end - start + 1 }));
      start = value;
      end = value;
    }
  }
  groups.push(Object.freeze({ start, end, count: end - start + 1 }));
  return groups;
}

function buildDarkMask(raw, width, height) {
  const mask = new Uint8Array(width * height);
  for (let index = 0; index < width * height; index += 1) {
    const offset = index * 4;
    if (luma(raw[offset], raw[offset + 1], raw[offset + 2]) < DARK_THRESHOLD) {
      mask[index] = 1;
    }
  }
  return mask;
}

function countDarkInRow(mask, width, y) {
  let count = 0;
  for (let x = 0; x < width; x += 1) {
    if (mask[(y * width) + x] === 1) count += 1;
  }
  return count;
}

function countDarkInColumnBand(mask, width, x, top, bottom) {
  let count = 0;
  for (let y = top; y <= bottom; y += 1) {
    if (mask[(y * width) + x] === 1) count += 1;
  }
  return count;
}

function expandGridRegion(raw, width, height, region) {
  let left = region.x;
  let right = region.x + region.width - 1;
  let top = region.y;
  let bottom = region.y + region.height - 1;
  const rowThreshold = Math.max(3, Math.round((right - left + 1) * 0.10));
  const columnThreshold = Math.max(3, Math.round((bottom - top + 1) * 0.08));
  let changed = true;
  while (changed) {
    changed = false;
    if (top > 0) {
      let dark = 0;
      for (let x = left; x <= right; x += 1) {
        const offset = ((top - 1) * width + x) * 4;
        if (luma(raw[offset], raw[offset + 1], raw[offset + 2]) < DARK_THRESHOLD) dark += 1;
      }
      if (dark >= rowThreshold) {
        top -= 1;
        changed = true;
      }
    }
    if (bottom < height - 1) {
      let dark = 0;
      for (let x = left; x <= right; x += 1) {
        const offset = ((bottom + 1) * width + x) * 4;
        if (luma(raw[offset], raw[offset + 1], raw[offset + 2]) < DARK_THRESHOLD) dark += 1;
      }
      if (dark >= rowThreshold) {
        bottom += 1;
        changed = true;
      }
    }
    if (left > 0) {
      let dark = 0;
      for (let y = top; y <= bottom; y += 1) {
        const offset = (y * width + left - 1) * 4;
        if (luma(raw[offset], raw[offset + 1], raw[offset + 2]) < DARK_THRESHOLD) dark += 1;
      }
      if (dark >= columnThreshold) {
        left -= 1;
        changed = true;
      }
    }
    if (right < width - 1) {
      let dark = 0;
      for (let y = top; y <= bottom; y += 1) {
        const offset = (y * width + right + 1) * 4;
        if (luma(raw[offset], raw[offset + 1], raw[offset + 2]) < DARK_THRESHOLD) dark += 1;
      }
      if (dark >= columnThreshold) {
        right += 1;
        changed = true;
      }
    }
  }
  return Object.freeze({
    x: left,
    y: top,
    width: right - left + 1,
    height: bottom - top + 1,
    area: (right - left + 1) * (bottom - top + 1),
  });
}

function detectGridLineRegions(raw, width, height) {
  const darkMask = buildDarkMask(raw, width, height);
  const rowSignals = [];
  for (let y = 0; y < height; y += 1) {
    if (countDarkInRow(darkMask, width, y) / Math.max(1, width) >= 0.12) {
      rowSignals.push(y);
    }
  }
  const rowGroups = groupSignals(rowSignals, 5)
    .filter((group) => group.count >= 8 || (group.end - group.start) >= height * 0.09);
  const regions = [];
  for (const rowGroup of rowGroups) {
    const top = clamp(rowGroup.start - 2, 0, height - 1);
    const bottom = clamp(rowGroup.end + 2, top, height - 1);
    const bandHeight = bottom - top + 1;
    const columnSignals = [];
    for (let x = 0; x < width; x += 1) {
      if (countDarkInColumnBand(darkMask, width, x, top, bottom) / Math.max(1, bandHeight) >= 0.10) {
        columnSignals.push(x);
      }
    }
    const columnGroups = groupSignals(columnSignals, 5)
      .filter((group) => group.count >= 6 || (group.end - group.start) >= width * 0.10);
    if (!columnGroups.length) continue;
    const left = columnGroups[0].start;
    const right = columnGroups[columnGroups.length - 1].end;
    const region = expandGridRegion(raw, width, height, {
      x: left,
      y: top,
      width: right - left + 1,
      height: bandHeight,
    });
    const score = scoreRegion(raw, width, height, region);
    const enoughRows = countLineSignals(raw, width, region).horizontalLineSignals >= 4;
    const enoughColumns = countLineSignals(raw, width, region).verticalLineSignals >= 2;
    if (region.width >= width * 0.16 && region.height >= height * 0.08 && enoughRows && enoughColumns) {
      regions.push(score);
    }
  }
  return regions;
}

function scoreRegion(raw, imageWidth, imageHeight, region) {
  const areaRatio = (region.width * region.height) / Math.max(1, imageWidth * imageHeight);
  const aspect = region.width / Math.max(1, region.height);
  const lines = countLineSignals(raw, imageWidth, region);
  const lineScore = lines.horizontalLineSignals + lines.verticalLineSignals;
  const aspectPenalty = aspect < 0.45 || aspect > 8 ? 0.35 : 1;
  const strong = areaRatio >= 0.015
    && region.width >= imageWidth * 0.12
    && region.height >= imageHeight * 0.05
    && lines.horizontalLineSignals >= 2
    && lines.verticalLineSignals >= 1;
  return Object.freeze({
    ...region,
    areaRatio,
    horizontalLineSignals: lines.horizontalLineSignals,
    verticalLineSignals: lines.verticalLineSignals,
    score: areaRatio * Math.max(1, lineScore) * aspectPenalty,
    strong,
  });
}

function toOriginalRegion(region, scale, originalWidth, originalHeight, marginRatio = 0.018) {
  const margin = Math.max(6, Math.round(Math.max(originalWidth, originalHeight) * marginRatio));
  const x = Math.floor(region.x / scale);
  const y = Math.floor(region.y / scale);
  const width = Math.ceil(region.width / scale);
  const height = Math.ceil(region.height / scale);
  const left = clamp(x - margin, 0, originalWidth - 1);
  const top = clamp(y - margin, 0, originalHeight - 1);
  const right = clamp(x + width + margin, left + 1, originalWidth);
  const bottom = clamp(y + height + margin, top + 1, originalHeight);
  return Object.freeze({
    x: left,
    y: top,
    width: right - left,
    height: bottom - top,
  });
}

export async function detectTableContextRegions({
  imageBase64,
  analysisWidth = ANALYSIS_WIDTH,
} = {}) {
  const buffer = decodeImageBase64(imageBase64);
  if (!buffer.length) {
    return Object.freeze({
      status: TABLE_CONTEXT_COMPOSITE_STATUS.NO_STRONG_TABLE_REGION,
      reason: "empty_image_input",
      regions: Object.freeze([]),
    });
  }
  const source = sharp(buffer, { limitInputPixels: false }).rotate();
  const metadata = await source.metadata();
  const originalWidth = Number(metadata.width);
  const originalHeight = Number(metadata.height);
  if (!Number.isFinite(originalWidth) || !Number.isFinite(originalHeight) || originalWidth <= 0 || originalHeight <= 0) {
    return Object.freeze({
      status: TABLE_CONTEXT_COMPOSITE_STATUS.NO_STRONG_TABLE_REGION,
      reason: "invalid_image_dimensions",
      regions: Object.freeze([]),
    });
  }
  const scaledWidth = Math.min(analysisWidth, originalWidth);
  const scale = scaledWidth / originalWidth;
  const { data, info } = await source
    .clone()
    .resize({ width: Math.round(scaledWidth), withoutEnlargement: true })
    .ensureAlpha()
    .raw()
    .toBuffer({ resolveWithObject: true });
  const baseMask = makeMask(data, info.width, info.height);
  const components = findComponents(dilateMask(baseMask, info.width, info.height, 2), info.width, info.height);
  const componentRegions = components
    .map((component) => scoreRegion(data, info.width, info.height, component))
    .filter((region) => region.strong);
  const gridRegions = componentRegions.length ? [] : detectGridLineRegions(data, info.width, info.height);
  const regions = [...componentRegions, ...gridRegions]
    .sort((a, b) => b.score - a.score)
    .map((region) => Object.freeze({
      analysisRegion: Object.freeze({
        x: region.x,
        y: region.y,
        width: region.width,
        height: region.height,
      }),
      region: toOriginalRegion(region, scale, originalWidth, originalHeight),
      score: region.score,
      areaRatio: region.areaRatio,
      horizontalLineSignals: region.horizontalLineSignals,
      verticalLineSignals: region.verticalLineSignals,
    }));
  return Object.freeze({
    status: regions.length ? TABLE_CONTEXT_COMPOSITE_STATUS.CREATED : TABLE_CONTEXT_COMPOSITE_STATUS.NO_STRONG_TABLE_REGION,
    reason: regions.length ? "strong_table_region_detected" : "no_strong_table_region",
    originalDimensions: Object.freeze({ width: originalWidth, height: originalHeight }),
    analysisDimensions: Object.freeze({ width: info.width, height: info.height }),
    regions: Object.freeze(regions),
  });
}

function makeContextRegions(originalDimensions = {}, tableRegion = {}) {
  const width = Number(originalDimensions.width);
  const height = Number(originalDimensions.height);
  if (!Number.isFinite(width) || !Number.isFinite(height)) return [];
  const stripHeight = clamp(Math.round(height * 0.085), 48, Math.round(height * 0.16));
  const bottom = Object.freeze({
    x: 0,
    y: Math.max(0, height - stripHeight),
    width,
    height: stripHeight,
    role: "bottom_context_strip",
  });
  const gap = Math.max(0, bottom.y - (Number(tableRegion.y) + Number(tableRegion.height)));
  if (gap < stripHeight * 0.25) return [bottom];
  return [bottom];
}

function toSharpExtract(region = {}) {
  return {
    left: Number(region.x) || 0,
    top: Number(region.y) || 0,
    width: Number(region.width) || 1,
    height: Number(region.height) || 1,
  };
}

export async function createTableContextComposite({
  imageBase64,
  mimeType = "image/jpeg",
  clock = Date.now,
} = {}) {
  const start = now(clock);
  try {
    const buffer = decodeImageBase64(imageBase64);
    const detection = await detectTableContextRegions({ imageBase64 });
    if (!buffer.length || detection.status !== TABLE_CONTEXT_COMPOSITE_STATUS.CREATED || !detection.regions.length) {
      return Object.freeze({
        status: TABLE_CONTEXT_COMPOSITE_STATUS.NO_STRONG_TABLE_REGION,
        reason: detection.reason || "no_strong_table_region",
        preprocessingMode: TABLE_CONTEXT_COMPOSITE_MODE,
        providerCalls: 0,
        preprocessingMs: Math.max(0, now(clock) - start),
        originalDimensions: detection.originalDimensions || null,
        detectedTableRegion: null,
        tableRegionPercentage: 0,
        contextRegions: Object.freeze([]),
      });
    }
    const source = sharp(buffer, { limitInputPixels: false }).rotate();
    const selected = detection.regions[0];
    const tableRegion = selected.region;
    const contextRegions = makeContextRegions(detection.originalDimensions, tableRegion);
    const padding = 18;
    const gap = 16;
    const compositeWidth = Math.max(
      tableRegion.width,
      ...contextRegions.map((region) => region.width),
    ) + (padding * 2);
    const compositeHeight = padding
      + tableRegion.height
      + (contextRegions.length ? gap : 0)
      + contextRegions.reduce((sum, region) => sum + region.height + gap, 0)
      + padding;
    const parts = [];
    parts.push({
      input: await source.clone().extract(toSharpExtract(tableRegion)).jpeg({ quality: 92 }).toBuffer(),
      left: padding,
      top: padding,
    });
    let top = padding + tableRegion.height + gap;
    for (const region of contextRegions) {
      parts.push({
        input: await source.clone().extract(toSharpExtract(region)).jpeg({ quality: 92 }).toBuffer(),
        left: padding,
        top,
      });
      top += region.height + gap;
    }
    const output = await sharp({
      create: {
        width: compositeWidth,
        height: compositeHeight,
        channels: 3,
        background: { r: 255, g: 255, b: 255 },
      },
    })
      .composite(parts)
      .jpeg({ quality: 92 })
      .toBuffer();
    return Object.freeze({
      status: TABLE_CONTEXT_COMPOSITE_STATUS.CREATED,
      reason: "composite_created",
      preprocessingMode: TABLE_CONTEXT_COMPOSITE_MODE,
      imageBase64: output.toString("base64"),
      mimeType: cleanMimeType("image/jpeg"),
      providerCalls: 0,
      preprocessingMs: Math.max(0, now(clock) - start),
      originalDimensions: detection.originalDimensions,
      detectedTableRegion: tableRegion,
      tableRegionPercentage: Number(((tableRegion.width * tableRegion.height) / (detection.originalDimensions.width * detection.originalDimensions.height) * 100).toFixed(4)),
      contextRegions: Object.freeze(contextRegions),
      compositeDimensions: Object.freeze({ width: compositeWidth, height: compositeHeight }),
      detection: Object.freeze({
        candidateRegionCount: detection.regions.length,
        selectedScore: selected.score,
        horizontalLineSignals: selected.horizontalLineSignals,
        verticalLineSignals: selected.verticalLineSignals,
      }),
    });
  } catch (error) {
    return Object.freeze({
      status: TABLE_CONTEXT_COMPOSITE_STATUS.ERROR,
      reason: String(error?.message || "preprocessing_error").slice(0, 160),
      preprocessingMode: TABLE_CONTEXT_COMPOSITE_MODE,
      providerCalls: 0,
      preprocessingMs: Math.max(0, now(clock) - start),
    });
  }
}

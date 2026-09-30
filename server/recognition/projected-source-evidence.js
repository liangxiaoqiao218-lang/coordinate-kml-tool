import { createHash } from "node:crypto";
import sharp from "sharp";

const PROJECTED_SOURCE_CONTEXT_SCHEMA = "projected_source_context_v1";
const LARGE_SOURCE_PIXEL_COUNT = 1_500_000;

function normalizedText(value) {
  return String(value || "").replace(/[\u00a0\u202f]/gu, " ").replace(/\r\n/gu, "\n");
}

function uniqueProjectedCrsIdentities(sourceText) {
  const identities = new Map();
  const source = normalizedText(sourceText);
  for (const match of source.matchAll(/\bEPSG\s*:?\s*(326|327)(\d{2})\b/giu)) {
    const zone = Number(match[2]);
    const hemisphere = match[1] === "326" ? "N" : "S";
    if (zone >= 1 && zone <= 60) {
      identities.set(`utm:${zone}:${hemisphere}`, Object.freeze({
        status: "EXPLICIT",
        id: `EPSG:${match[1]}${match[2]}`,
        projection: "utm",
        zone,
        hemisphere,
        sourceText: String(match[0] || "").trim()
      }));
    }
  }
  for (const match of source.matchAll(/\bUTM\b[^\r\n]{0,100}?\b(?:ZONE|ZONA)\s*(\d{1,2})\s*([NS])\b/giu)) {
    const zone = Number(match[1]);
    const hemisphere = String(match[2] || "").toUpperCase();
    if (zone >= 1 && zone <= 60) {
      identities.set(`utm:${zone}:${hemisphere}`, Object.freeze({
        status: "EXPLICIT",
        id: `EPSG:${hemisphere === "N" ? "326" : "327"}${String(zone).padStart(2, "0")}`,
        projection: "utm",
        zone,
        hemisphere,
        sourceText: String(match[0] || "").trim()
      }));
    }
  }
  for (const match of source.matchAll(/\bUTM\s*(\d{1,2})\s*([NS])\b/giu)) {
    const zone = Number(match[1]);
    const hemisphere = String(match[2] || "").toUpperCase();
    if (zone >= 1 && zone <= 60) {
      identities.set(`utm:${zone}:${hemisphere}`, Object.freeze({
        status: "EXPLICIT",
        id: `EPSG:${hemisphere === "N" ? "326" : "327"}${String(zone).padStart(2, "0")}`,
        projection: "utm",
        zone,
        hemisphere,
        sourceText: String(match[0] || "").trim()
      }));
    }
  }
  if (/\bBFTM\b/iu.test(source)) {
    identities.set("bftm", Object.freeze({
      status: "EXPLICIT",
      id: "EPSG:3440",
      projection: "bftm",
      zone: null,
      hemisphere: "N",
      sourceText: "BFTM"
    }));
  }
  return identities;
}

function projectedAxisOrders(sourceText) {
  const orders = new Set();
  for (const rawLine of normalizedText(sourceText).split("\n")) {
    const line = rawLine.replace(/[|;,:/()[\]{}._-]+/gu, " ").replace(/\s+/gu, " ").trim();
    if (!line) continue;
    const easting = line.search(/\b(?:EASTING|X)\b/iu);
    const northing = line.search(/\b(?:NORTHING|Y)\b/iu);
    if (easting < 0 || northing < 0 || easting === northing) continue;
    orders.add(easting < northing ? "easting_northing" : "northing_easting");
  }
  return orders;
}

export function extractProjectedSourceContext(sourceText = "") {
  const crsIdentities = uniqueProjectedCrsIdentities(sourceText);
  const axisOrders = projectedAxisOrders(sourceText);
  const crsConflict = crsIdentities.size > 1;
  const axisConflict = axisOrders.size > 1;
  const crsEvidence = crsIdentities.size === 1 ? [...crsIdentities.values()][0] : null;
  const axisOrder = axisOrders.size === 1 ? [...axisOrders][0] : null;
  return Object.freeze({
    schema_version: PROJECTED_SOURCE_CONTEXT_SCHEMA,
    status: crsEvidence && axisOrder && !crsConflict && !axisConflict ? "COMPLETE" : "INCOMPLETE",
    crsEvidence,
    axisOrder,
    crsConflict,
    axisConflict,
    crsIdentityCount: crsIdentities.size,
    axisOrderCount: axisOrders.size
  });
}

function sameCrs(left, right) {
  if (!left || !right) return true;
  return String(left.projection || "").toLowerCase() === String(right.projection || "").toLowerCase()
    && Number(left.zone || 0) === Number(right.zone || 0)
    && String(left.hemisphere || "").toUpperCase() === String(right.hemisphere || "").toUpperCase();
}

export function bindProjectedEvidenceToSourceContext({
  providerEvidence = null,
  sourceContextText = "",
  sourceContextProvenance = null,
  imageIdentity = null
} = {}) {
  if (!providerEvidence || typeof providerEvidence !== "object") return providerEvidence;
  const context = extractProjectedSourceContext(sourceContextText);
  const imageSha256 = String(imageIdentity?.image_sha256 || "");
  const provenanceSha256 = String(sourceContextProvenance?.image_sha256 || "");
  const sourceBound = /^[0-9a-f]{64}$/u.test(imageSha256)
    && provenanceSha256 === imageSha256
    && sourceContextProvenance?.same_request === true;
  const providerCrs = String(providerEvidence?.crsEvidence?.status || "").toUpperCase() === "EXPLICIT"
    ? providerEvidence.crsEvidence
    : null;
  const providerAxisOrder = providerEvidence?.diagnostics?.headerPresent === true
    && ["easting_northing", "northing_easting"].includes(String(providerEvidence?.axisOrder || ""))
    ? String(providerEvidence.axisOrder)
    : null;
  if (!sourceContextText.trim() && providerCrs && providerEvidence.diagnostics?.headerPresent === true) {
    return providerEvidence;
  }
  const crsConflict = context.crsConflict || (providerCrs && context.crsEvidence
    ? !sameCrs(providerCrs, context.crsEvidence)
    : false);
  const axisConflict = context.axisConflict || Boolean(
    providerAxisOrder && context.axisOrder && providerAxisOrder !== context.axisOrder
  );
  const selectedCrs = crsConflict ? null : (providerCrs || context.crsEvidence);
  const selectedAxisOrder = axisConflict ? null : (providerAxisOrder || context.axisOrder);
  const bound = sourceBound && !crsConflict && !axisConflict && Boolean(selectedCrs && selectedAxisOrder);
  return Object.freeze({
    ...providerEvidence,
    crsEvidence: bound ? selectedCrs : Object.freeze({
      status: crsConflict ? "CONFLICT" : "UNCONFIRMED",
      projection: "",
      zone: null,
      hemisphere: ""
    }),
    axisOrder: bound ? selectedAxisOrder : null,
    sourceContextBinding: Object.freeze({
      schema_version: PROJECTED_SOURCE_CONTEXT_SCHEMA,
      bound,
      image_sha256: sourceBound ? imageSha256 : null,
      mode: String(sourceContextProvenance?.mode || ""),
      crsSource: bound ? (providerCrs ? "provider" : "local_ocr") : null,
      axisSource: bound ? (providerAxisOrder ? "provider_header" : "local_ocr") : null,
      crsConflict,
      axisConflict
    }),
    diagnostics: Object.freeze({
      ...(providerEvidence.diagnostics || {}),
      headerPresent: providerEvidence.diagnostics?.headerPresent === true || (bound && Boolean(context.axisOrder)),
      sourceContextBound: bound,
      sourceContextCrsConflict: crsConflict,
      sourceContextAxisConflict: axisConflict
    })
  });
}

export async function createLocalOcrClassificationImage({ imageBuffer, imageIdentity } = {}) {
  if (!Buffer.isBuffer(imageBuffer) || imageBuffer.length === 0) {
    throw new TypeError("local_ocr_image_buffer_required");
  }
  const metadata = await sharp(imageBuffer, { failOn: "warning" }).metadata();
  const width = Number(metadata.width) || 0;
  const height = Number(metadata.height) || 0;
  const pixelCount = width * height;
  const baseProvenance = {
    schema_version: PROJECTED_SOURCE_CONTEXT_SCHEMA,
    image_sha256: String(imageIdentity?.image_sha256 || ""),
    source_width: width,
    source_height: height,
    same_request: true
  };
  if (width < 900 || height < 500 || pixelCount < LARGE_SOURCE_PIXEL_COUNT) {
    return Object.freeze({
      image: imageBuffer,
      layoutSafe: true,
      provenance: Object.freeze({ ...baseProvenance, mode: "source_image" })
    });
  }

  const overviewWidth = Math.min(1100, width);
  const detailWidth = Math.min(2200, Math.max(width, Math.round(width * 1.4)));
  const footerHeight = Math.max(1, Math.min(height, Math.round(height * 0.28)));
  const overview = await sharp(imageBuffer, { failOn: "warning" })
    .resize({ width: overviewWidth, withoutEnlargement: true, kernel: "lanczos3" })
    .grayscale().normalize().sharpen().png().toBuffer({ resolveWithObject: true });
  const footer = await sharp(imageBuffer, { failOn: "warning" })
    .extract({ left: 0, top: height - footerHeight, width, height: footerHeight })
    .resize({ width: detailWidth, kernel: "lanczos3" })
    .grayscale().normalize().sharpen().png().toBuffer({ resolveWithObject: true });
  const canvasWidth = Math.max(overview.info.width, footer.info.width);
  const separator = 30;
  const image = await sharp({
    create: {
      width: canvasWidth,
      height: overview.info.height + separator + footer.info.height,
      channels: 3,
      background: "#ffffff"
    }
  }).composite([
    { input: overview.data, left: 0, top: 0 },
    { input: footer.data, left: 0, top: overview.info.height + separator }
  ]).png().toBuffer();
  return Object.freeze({
    image,
    layoutSafe: false,
    provenance: Object.freeze({
      ...baseProvenance,
      mode: "overview_footer_composite",
      overview_width: overview.info.width,
      footer_top: height - footerHeight,
      footer_height: footerHeight,
      footer_output_width: footer.info.width,
      artifact_sha256: createHash("sha256").update(image).digest("hex")
    })
  });
}

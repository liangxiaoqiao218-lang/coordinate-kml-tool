import sharp from "sharp";

export async function buildProjectedTableVisionTiles(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length === 0) return [];
  const source = sharp(buffer, { failOn: "none" });
  const metadata = await source.metadata();
  const width = Number(metadata.width);
  const height = Number(metadata.height);
  if (!Number.isInteger(width) || !Number.isInteger(height) || width < 400 || height < 400) return [];

  const tileWidth = Math.min(width, Math.ceil(width * 0.62));
  const tileHeight = Math.min(height, Math.ceil(height * 0.62));
  const positions = [
    { left: 0, top: 0 },
    { left: width - tileWidth, top: 0 },
    { left: 0, top: height - tileHeight },
    { left: width - tileWidth, top: height - tileHeight }
  ];
  const uniquePositions = positions.filter((position, index, values) => (
    values.findIndex(value => value.left === position.left && value.top === position.top) === index
  ));

  return Promise.all(uniquePositions.map(async position => {
    const tile = await sharp(buffer, { failOn: "none" })
      .extract({ ...position, width: tileWidth, height: tileHeight })
      .resize({ width: Math.min(2400, tileWidth * 2), withoutEnlargement: false })
      .jpeg({ quality: 94, chromaSubsampling: "4:4:4" })
      .toBuffer();
    return {
      type: "image_url",
      image_url: {
        url: `data:image/jpeg;base64,${tile.toString("base64")}`,
        detail: "high"
      }
    };
  }));
}

export async function buildProjectedCoordinateTableVisionTiles(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length === 0) return [];
  const metadata = await sharp(buffer, { failOn: "none" }).metadata();
  const width = Number(metadata.width);
  const height = Number(metadata.height);
  if (!Number.isInteger(width) || !Number.isInteger(height) || width < 400 || height < 400) return [];

  const cropWidth = Math.min(width, Math.ceil(width * 0.48));
  const cropHeight = Math.min(height, Math.ceil(height * 0.42));
  const top = Math.max(0, Math.floor(height * 0.50));
  const crops = [
    { left: 0, top, width: cropWidth, height: Math.min(cropHeight, height - top) }
  ].filter(crop => crop.width > 0 && crop.height > 0);

  return Promise.all(crops.map(async crop => {
    const tile = await sharp(buffer, { failOn: "none" })
      .extract(crop)
      .resize({ width: Math.min(3200, crop.width * 3), withoutEnlargement: false })
      .jpeg({ quality: 96, chromaSubsampling: "4:4:4" })
      .toBuffer();
    return {
      type: "image_url",
      image_url: {
        url: `data:image/jpeg;base64,${tile.toString("base64")}`,
        detail: "high"
      }
    };
  }));
}

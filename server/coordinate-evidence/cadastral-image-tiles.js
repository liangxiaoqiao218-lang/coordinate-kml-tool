import sharp from "sharp";

export async function buildMadagascarCadastralTableVisionTiles(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length === 0) return [];
  const metadata = await sharp(buffer, { failOn: "none" }).metadata();
  const width = Number(metadata.width);
  const height = Number(metadata.height);
  if (!Number.isInteger(width) || !Number.isInteger(height) || width < 400 || height < 400) return [];

  const crops = [
    {
      left: Math.max(0, Math.floor(width * 0.72)),
      top: Math.max(0, Math.floor(height * 0.04)),
      width: Math.max(1, width - Math.max(0, Math.floor(width * 0.72))),
      height: Math.max(1, Math.floor(height * 0.92))
    },
    {
      left: Math.max(0, Math.floor(width * 0.62)),
      top: 0,
      width: Math.max(1, width - Math.max(0, Math.floor(width * 0.62))),
      height
    }
  ];

  const normalizedCrops = crops
    .map(crop => ({
      left: Math.min(Math.max(0, crop.left), width - 1),
      top: Math.min(Math.max(0, crop.top), height - 1),
      width: Math.min(crop.width, width - crop.left),
      height: Math.min(crop.height, height - crop.top)
    }))
    .filter(crop => crop.width > 0 && crop.height > 0);

  return Promise.all(normalizedCrops.map(async crop => {
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

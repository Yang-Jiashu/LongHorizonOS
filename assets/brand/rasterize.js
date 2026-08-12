/**
 * Rasterize the LongHorizonOS brand SVGs into PNG exports.
 *
 * Requires `sharp`. If it is not on the default resolution path, point NODE_PATH
 * at a node_modules directory that provides it:
 *
 *   node assets/brand/rasterize.js
 */
"use strict";

const fs = require("fs");
const path = require("path");

let sharp;
try {
  sharp = require("sharp");
} catch (err) {
  console.error(
    "Could not load `sharp`. Install it (npm i -D sharp) or set NODE_PATH " +
      "to a node_modules directory that provides it."
  );
  process.exit(1);
}

const BRAND = __dirname;
const PNG = path.join(BRAND, "png");

// [source svg, output name, width, height or null to preserve aspect]
const EXPORTS = [
  ["icon.svg", "icon-1024.png", 1024, 1024],
  ["icon.svg", "icon-512.png", 512, 512],
  ["icon.svg", "icon-256.png", 256, 256],
  ["icon.svg", "icon-128.png", 128, 128],
  ["icon.svg", "favicon-64.png", 64, 64],
  ["icon.svg", "favicon-32.png", 32, 32],
  ["icon.svg", "favicon-16.png", 16, 16],
  ["icon-dark.svg", "icon-dark-512.png", 512, 512],
  ["icon-dark.svg", "icon-dark-256.png", 256, 256],
  ["logo-horizontal-on-light.svg", "logo-horizontal-on-light.png", 1200, null],
  ["logo-horizontal-on-dark.svg", "logo-horizontal-on-dark.png", 1200, null],
  ["logo-stacked-on-light.svg", "logo-stacked-on-light.png", 800, null],
  ["logo-stacked-on-dark.svg", "logo-stacked-on-dark.png", 800, null],
  ["mark-on-dark.svg", "mark-on-dark.png", 512, null],
  ["mark-on-light.svg", "mark-on-light.png", 512, null],
  ["banner.svg", "banner.png", 1280, null],
];

async function main() {
  fs.mkdirSync(PNG, { recursive: true });
  for (const [src, outName, width, height] of EXPORTS) {
    const resize = height ? { width, height, fit: "contain" } : { width };
    await sharp(path.join(BRAND, src), { density: 600 })
      .resize(resize)
      .png({ compressionLevel: 9 })
      .toFile(path.join(PNG, outName));
    console.log("wrote png/" + outName);
  }
}

main().catch((err) => {
  console.error(err.message);
  process.exit(1);
});

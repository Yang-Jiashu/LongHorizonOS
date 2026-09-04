# LongHorizonOS brand assets

## The mark

Three ascending steps ending in a filled dot. It is a literal picture of what
the Verified Progress Graph does:

| Element | Meaning |
|---|---|
| Two dim steps | Preserved `VERIFIED` work — a change elsewhere does not rerun it |
| Bright step | The advancing readiness / repair frontier |
| Mint dot | Current, applicable **Evidence** — the only thing that closes a Goal |

The steps read left-to-right and bottom-to-top, so the mark carries the
"long horizon" idea (durable progress over time) without drawing a literal
horizon line. Value contrast does the storytelling, which is why the mark
survives at favicon size and in one colour.

## Palette

| Token | Hex | Use |
|---|---|---|
| Ink | `#0B0F2A` | Wordmark on light, mono mark on light |
| Tile top | `#171E52` | Icon / banner gradient start |
| Tile bottom | `#090C22` | Icon / banner gradient end |
| Step 1 | `#3E4A80` | Preserved work, furthest back (on dark) |
| Step 2 | `#5B6AA8` | Preserved work, nearer (on dark) |
| Frontier | `#FFFFFF` | The advancing frontier (on dark) |
| Evidence mint | `#22D3A6` | Evidence, closure, and the `OS` in the wordmark |

On light backgrounds the preserved steps become `#A6AFD2` and `#5B6AA8`, and the
frontier step becomes Ink, to hold the same value ladder.

Mint means *verified* and nothing else. Do not use it for decoration — the
moment mint appears on unverified state, the mark stops telling the truth about
the system.

## Files

| File | Use |
|---|---|
| `logo-horizontal-on-light.svg` / `-on-dark.svg` | Default lockup: docs headers, sites, slides |
| `logo-stacked-on-light.svg` / `-on-dark.svg` | Square-ish spaces, conference cards, stickers |
| `mark-on-light.svg` / `mark-on-dark.svg` | Mark alone, where the name is already present |
| `mark-mono-white.svg` / `mark-mono-black.svg` | One-colour contexts: print, embroidery, watermarks |
| `icon.svg` | Rounded-tile app icon / avatar (GitHub org, social) |
| `banner.svg` | Wide README / social preview header |
| `png/` | Rasterized exports, including 16/32/64px favicons |

## Clear space and minimum size

Keep clear space of at least the height of one step (about 25% of the mark's
height) on all sides. The artboards already include this padding, so exporting
at the given viewBox is safe.

Minimum sizes: 16px for `icon.svg`, and 120px wide for the horizontal lockup —
below that, switch to the mark alone.

## Don't

- Don't recolour the mint dot, or add a second accent colour.
- Don't reorder or add steps; three is the ratio the geometry is tuned for.
- Don't add drop shadows, outlines, or bevels.
- Don't stretch — scale proportionally.
- Don't set the wordmark in another typeface. Use the provided SVGs; the
  wordmark is already converted to outlines, so no font install is required.

## Regenerating

The SVGs are generated, not hand-edited. Edit `build_brand.py` and rerun:

```bash
python -m pip install fonttools
python assets/brand/build_brand.py     # SVG (wordmark outlined)
node assets/brand/rasterize.js         # PNG + favicon exports (needs sharp)
```

The wordmark is Ubuntu (Medium for "LongHorizon", Bold for "OS"), licensed under
the Ubuntu Font Licence 1.0, converted to outlines at build time. `build_brand.py`
looks for `Ubuntu-M.ttf` and `Ubuntu-B.ttf` in `assets/brand/fonts/` first, so
drop the two files there to make the build reproducible on any machine.

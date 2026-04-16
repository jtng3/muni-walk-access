/**
 * Find the "pole of inaccessibility" — the interior point farthest from any
 * polygon edge. Much better than centroid for label placement on irregular or
 * concave polygons (centroid can land outside the shape).
 *
 * Algorithm: iterative grid refinement (Mapbox's polylabel approach, inlined
 * to avoid an external dependency).
 *
 * @param polygon – GeoJSON-style rings: number[][][] (outer ring + holes)
 * @param precision – stop refining when potential improvement < this value
 *                    (in the same units as the coordinates — degrees for geo)
 * @returns [x, y] of the visual center
 */

// Signed distance from point to polygon boundary (positive = inside)
function pointToPolygonDist(
  px: number,
  py: number,
  polygon: number[][][],
): number {
  let inside = false;
  let minDistSq = Infinity;

  for (const ring of polygon) {
    for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
      const [ax, ay] = ring[i];
      const [bx, by] = ring[j];

      // Ray-casting parity test
      if (
        ay > py !== by > py &&
        px < ((bx - ax) * (py - ay)) / (by - ay) + ax
      ) {
        inside = !inside;
      }

      // Closest point on segment → squared distance
      const dx = bx - ax;
      const dy = by - ay;
      const lenSq = dx * dx + dy * dy;
      const t =
        lenSq === 0
          ? 0
          : Math.max(0, Math.min(1, ((px - ax) * dx + (py - ay) * dy) / lenSq));
      const cx = ax + t * dx - px;
      const cy = ay + t * dy - py;
      minDistSq = Math.min(minDistSq, cx * cx + cy * cy);
    }
  }

  return (inside ? 1 : -1) * Math.sqrt(minDistSq);
}

interface Cell {
  x: number;
  y: number;
  h: number; // half-size
  d: number; // signed distance to polygon edge
  max: number; // upper bound on best distance in this cell
}

function makeCell(
  x: number,
  y: number,
  h: number,
  polygon: number[][][],
): Cell {
  const d = pointToPolygonDist(x, y, polygon);
  return { x, y, h, d, max: d + h * Math.SQRT2 };
}

export default function polylabel(
  polygon: number[][][],
  precision = 0.000001,
): [number, number] {
  // Bounding box
  let minX = Infinity,
    minY = Infinity,
    maxX = -Infinity,
    maxY = -Infinity;
  for (const [x, y] of polygon[0]) {
    if (x < minX) minX = x;
    if (y < minY) minY = y;
    if (x > maxX) maxX = x;
    if (y > maxY) maxY = y;
  }

  const width = maxX - minX;
  const height = maxY - minY;
  const cellSize = Math.max(width, height);
  if (cellSize === 0) return [minX, minY];

  const h = cellSize / 2;

  // Seed grid
  const queue: Cell[] = [];
  for (let x = minX; x < maxX; x += cellSize) {
    for (let y = minY; y < maxY; y += cellSize) {
      queue.push(makeCell(x + h, y + h, h, polygon));
    }
  }

  // Bbox center as initial best
  let best = makeCell((minX + maxX) / 2, (minY + maxY) / 2, 0, polygon);

  while (queue.length) {
    // Pop cell with highest potential (simple linear scan — fine for ≤41 polygons)
    let bi = 0;
    for (let i = 1; i < queue.length; i++) {
      if (queue[i].max > queue[bi].max) bi = i;
    }
    const cell = queue[bi];
    queue[bi] = queue[queue.length - 1];
    queue.pop();

    if (cell.d > best.d) best = cell;
    if (cell.max - best.d <= precision) continue;

    const h2 = cell.h / 2;
    queue.push(
      makeCell(cell.x - h2, cell.y - h2, h2, polygon),
      makeCell(cell.x + h2, cell.y - h2, h2, polygon),
      makeCell(cell.x - h2, cell.y + h2, h2, polygon),
      makeCell(cell.x + h2, cell.y + h2, h2, polygon),
    );
  }

  return [best.x, best.y];
}

import { createGeometryRenderPlan } from "./geometry-render-plan.js";

function svgElement(name, attributes = {}) {
  const element = document.createElementNS("http://www.w3.org/2000/svg", name);
  Object.entries(attributes).forEach(([key, value]) => element.setAttribute(key, String(value)));
  return element;
}

function projectedGroups(geometry, bounds, width, height, padding) {
  const { west, south, east, north } = bounds;
  const rangeX = Math.max(east - west, 0.000001);
  const rangeY = Math.max(north - south, 0.000001);
  const project = ([x, y]) => [
    west === east ? width / 2 : padding + ((x - west) / rangeX) * (width - padding * 2),
    south === north ? height / 2 : height - padding - ((y - south) / rangeY) * (height - padding * 2)
  ];
  if (geometry.type === "Point") return [[project(geometry.coordinates)]];
  if (geometry.type === "LineString") return [geometry.coordinates.map(project)];
  if (geometry.type === "Polygon") return geometry.coordinates.map(ring => ring.map(project));
  return geometry.coordinates.flatMap(polygon => polygon.map(ring => ring.map(project)));
}

export class LocalSvgRenderer {
  constructor({ container, attributionElement }) {
    this.container = container;
    this.attributionElement = attributionElement;
    this.plan = null;
  }

  async render(geometry) {
    this.plan = createGeometryRenderPlan(geometry);
    const width = 900;
    const height = 560;
    const svg = svgElement("svg", {
      viewBox: `0 0 ${width} ${height}`,
      role: "img",
      "aria-label": "本地坐标 Geometry 预览"
    });
    svg.append(svgElement("rect", { x: 0, y: 0, width, height, fill: "#edf2f6" }));
    const groups = projectedGroups(geometry, this.plan.bounds, width, height, 52);
    if (geometry.type === "Point") {
      const [x, y] = groups[0][0];
      svg.append(svgElement("circle", {
        cx: x, cy: y, r: 9, fill: "#1976D2", stroke: "#E53935", "stroke-width": 3
      }));
    } else {
      groups.forEach(points => {
        const values = points.map(point => point.join(",")).join(" ");
        svg.append(svgElement(geometry.type === "LineString" ? "polyline" : "polygon", {
          points: values,
          fill: geometry.type === "LineString" ? "none" : "#1976D2",
          "fill-opacity": geometry.type === "LineString" ? 0 : 0.15,
          stroke: "#E53935",
          "stroke-width": 3,
          "vector-effect": "non-scaling-stroke"
        }));
      });
    }
    this.container.hidden = false;
    this.container.replaceChildren(svg);
    if (this.attributionElement) {
      this.attributionElement.textContent = "本地 SVG 预览 · 未请求底图服务";
      this.attributionElement.hidden = false;
    }
  }
}

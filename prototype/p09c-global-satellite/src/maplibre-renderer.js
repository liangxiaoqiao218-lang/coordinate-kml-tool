import { createGeometryRenderPlan } from "./geometry-render-plan.js";

const ATTRIBUTION = "© MapTiler © OpenStreetMap contributors";

function mapLoad(map) {
  return new Promise((resolve, reject) => {
    map.once("load", resolve);
    map.once("error", event => reject(Object.assign(new Error("MAPLIBRE_LOAD_FAILED"), {
      code: "MAPLIBRE_LOAD_FAILED",
      cause: event?.error
    })));
  });
}

export class MapLibreRenderer {
  constructor({ maplibregl, container, attributionElement }) {
    this.maplibregl = maplibregl;
    this.container = container;
    this.attributionElement = attributionElement;
    this.map = null;
    this.plan = null;
  }

  async render({ geometry, styleUrl }) {
    this.destroy();
    this.plan = createGeometryRenderPlan(geometry);
    this.map = new this.maplibregl.Map({
      container: this.container,
      style: styleUrl,
      center: [0, 0],
      zoom: 1,
      attributionControl: false
    });
    this.map.addControl(new this.maplibregl.NavigationControl(), "top-right");
    this.map.addControl(new this.maplibregl.ScaleControl({ unit: "metric", maxWidth: 120 }), "bottom-left");
    await mapLoad(this.map);
    this.addCanonicalGeometry();
    this.fitGeometry();
    this.attributionElement.textContent = ATTRIBUTION;
    this.attributionElement.hidden = false;
  }

  addCanonicalGeometry() {
    if (!this.map || !this.plan) return;
    if (this.map.getSource(this.plan.sourceId)) return;
    this.map.addSource(this.plan.sourceId, this.plan.source);
    this.plan.layers.forEach(layer => this.map.addLayer(layer));
  }

  fitGeometry() {
    const { west, south, east, north } = this.plan.bounds;
    if (west === east && south === north) {
      this.map.easeTo({ center: [west, south], zoom: 15, duration: 0 });
      return;
    }
    this.map.fitBounds([[west, south], [east, north]], { padding: 54, maxZoom: 17, duration: 0 });
  }

  async switchStyle(styleUrl) {
    if (!this.map) throw new Error("MAP_NOT_READY");
    const ready = new Promise((resolve, reject) => {
      this.map.once("style.load", resolve);
      this.map.once("error", event => reject(event?.error || new Error("STYLE_LOAD_FAILED")));
    });
    this.map.setStyle(styleUrl);
    await ready;
    this.addCanonicalGeometry();
    this.fitGeometry();
  }

  destroy() {
    this.map?.remove();
    this.map = null;
  }
}

function svgElement(name, attributes = {}) {
  const element = document.createElementNS("http://www.w3.org/2000/svg", name);
  Object.entries(attributes).forEach(([key, value]) => element.setAttribute(key, String(value)));
  return element;
}

function projectedPositions(geometry, bounds, width, height, padding) {
  const { west, south, east, north } = bounds;
  const rangeX = Math.max(east - west, 0.000001);
  const rangeY = Math.max(north - south, 0.000001);
  const project = ([x, y]) => [
    padding + ((x - west) / rangeX) * (width - padding * 2),
    height - padding - ((y - south) / rangeY) * (height - padding * 2)
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
  }

  async render(geometry) {
    const plan = createGeometryRenderPlan(geometry);
    const width = 900;
    const height = 560;
    const svg = svgElement("svg", { viewBox: `0 0 ${width} ${height}`, role: "img", "aria-label": "Local geometry fallback" });
    svg.append(svgElement("rect", { x: 0, y: 0, width, height, fill: "#edf2f6" }));
    const groups = projectedPositions(geometry, plan.bounds, width, height, 52);
    if (geometry.type === "Point") {
      const [x, y] = groups[0][0];
      svg.append(svgElement("circle", { cx: x, cy: y, r: 9, fill: "#1976D2", stroke: "#E53935", "stroke-width": 3 }));
    } else {
      groups.forEach(points => {
        const value = points.map(point => point.join(",")).join(" ");
        svg.append(svgElement(geometry.type === "LineString" ? "polyline" : "polygon", {
          points: value,
          fill: geometry.type === "LineString" ? "none" : "#1976D2",
          "fill-opacity": geometry.type === "LineString" ? 0 : 0.15,
          stroke: "#E53935",
          "stroke-width": 3,
          "vector-effect": "non-scaling-stroke"
        }));
      });
    }
    this.container.replaceChildren(svg);
    this.attributionElement.textContent = "Local SVG fallback · no basemap provider request";
    this.attributionElement.hidden = false;
  }
}

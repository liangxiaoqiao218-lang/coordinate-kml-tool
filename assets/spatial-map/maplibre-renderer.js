import { createGeometryRenderPlan } from "./geometry-render-plan.js";

const WEB_MERCATOR_RADIUS = 6378137;
const WEB_MERCATOR_MAX_LATITUDE = 85.05112878;
export const LOCAL_MAP_PADDING_RATIO = 0.1;
export const LOCAL_MAP_MIN_RELATIVE_ZOOM = 1;
export const LOCAL_MAP_MAX_RELATIVE_ZOOM = 20;

function svgElement(name, attributes = {}) {
  const element = document.createElementNS("http://www.w3.org/2000/svg", name);
  Object.entries(attributes).forEach(([key, value]) => element.setAttribute(key, String(value)));
  return element;
}

function mapPositions(value, project) {
  if (Array.isArray(value) && value.length >= 2 && Number.isFinite(value[0]) && Number.isFinite(value[1])) return project(value);
  return Array.isArray(value) ? value.map(item => mapPositions(item, project)) : value;
}

function flattenedPositions(geometry) {
  if (geometry.type === "Point") return [geometry.coordinates];
  if (geometry.type === "LineString") return geometry.coordinates;
  if (geometry.type === "Polygon") return geometry.coordinates.flat(1);
  return geometry.coordinates.flat(2);
}

export function projectWebMercatorPosition(position) {
  const longitude = Number(position[0]);
  const latitude = Math.max(-WEB_MERCATOR_MAX_LATITUDE, Math.min(WEB_MERCATOR_MAX_LATITUDE, Number(position[1])));
  const longitudeRadians = longitude * Math.PI / 180;
  const latitudeRadians = latitude * Math.PI / 180;
  return [
    WEB_MERCATOR_RADIUS * longitudeRadians,
    WEB_MERCATOR_RADIUS * Math.log(Math.tan(Math.PI / 4 + latitudeRadians / 2))
  ];
}

export function projectWgs84GeometryForDisplay(geometry) {
  return { type: geometry.type, coordinates: mapPositions(geometry.coordinates, projectWebMercatorPosition) };
}

export function projectedGeometryBounds(geometry) {
  return flattenedPositions(geometry).reduce((bounds, [x, y]) => ({
    minX: Math.min(bounds.minX, x), minY: Math.min(bounds.minY, y),
    maxX: Math.max(bounds.maxX, x), maxY: Math.max(bounds.maxY, y)
  }), { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity });
}

export function createLocalFitTransform(projectedGeometry, width, height, paddingRatio = LOCAL_MAP_PADDING_RATIO) {
  const safeWidth = Math.max(1, Number(width) || 1);
  const safeHeight = Math.max(1, Number(height) || 1);
  const padding = Math.max(0, Math.min(0.25, Number(paddingRatio) || 0));
  const availableWidth = safeWidth * (1 - padding * 2);
  const availableHeight = safeHeight * (1 - padding * 2);
  const bounds = projectedGeometryBounds(projectedGeometry);
  const spanX = Math.max(0, bounds.maxX - bounds.minX);
  const spanY = Math.max(0, bounds.maxY - bounds.minY);
  const scaleX = spanX > 0 ? availableWidth / spanX : Infinity;
  const scaleY = spanY > 0 ? availableHeight / spanY : Infinity;
  const candidate = Math.min(scaleX, scaleY);
  const scale = Number.isFinite(candidate) ? candidate : Number.isFinite(scaleX) ? scaleX : Number.isFinite(scaleY) ? scaleY : 1;
  const safeScale = Number.isFinite(scale) && scale > 0 ? scale : 1;
  const centerX = (bounds.minX + bounds.maxX) / 2;
  const centerY = (bounds.minY + bounds.maxY) / 2;
  return Object.freeze({
    scale: safeScale,
    translateX: safeWidth / 2 - centerX * safeScale,
    translateY: safeHeight / 2 + centerY * safeScale,
    width: safeWidth,
    height: safeHeight,
    paddingRatio: padding,
    bounds: Object.freeze(bounds)
  });
}

export function applyLocalViewTransform(position, transform) {
  return [position[0] * transform.scale + transform.translateX, -position[1] * transform.scale + transform.translateY];
}

function geometryGroups(geometry) {
  if (geometry.type === "Point") return [[geometry.coordinates]];
  if (geometry.type === "LineString") return [geometry.coordinates];
  if (geometry.type === "Polygon") return geometry.coordinates;
  return geometry.coordinates.flatMap(polygon => polygon);
}

export class LocalSvgRenderer {
  constructor({ container, attributionElement }) {
    this.container = container;
    this.attributionElement = attributionElement;
    this.plan = null;
    this.svg = null;
    this.projectedGeometry = null;
    this.fitTransform = null;
    this.viewTransform = null;
    this.shapes = [];
    this.pointerPositions = new Map();
    this.pinchDistance = null;
    this.boundHandlers = null;
  }

  viewportSize() {
    const bounds = this.container?.getBoundingClientRect?.() || {};
    return {
      width: Math.max(1, Number(bounds.width) || Number(this.container?.clientWidth) || 900),
      height: Math.max(1, Number(bounds.height) || Number(this.container?.clientHeight) || 560)
    };
  }

  updateShapes() {
    if (!this.projectedGeometry || !this.viewTransform) return;
    geometryGroups(this.projectedGeometry).forEach((points, index) => {
      const screen = points.map(position => applyLocalViewTransform(position, this.viewTransform));
      const shape = this.shapes[index];
      if (!shape) return;
      if (this.projectedGeometry.type === "Point") {
        shape.setAttribute("cx", String(screen[0][0]));
        shape.setAttribute("cy", String(screen[0][1]));
      } else {
        shape.setAttribute("points", screen.map(point => point.join(",")).join(" "));
      }
    });
  }

  panBy(deltaX, deltaY) {
    if (!this.viewTransform) return false;
    this.viewTransform.translateX += Number(deltaX) || 0;
    this.viewTransform.translateY += Number(deltaY) || 0;
    this.updateShapes();
    return true;
  }

  zoomAt(relativeFactor, screenX, screenY) {
    if (!this.viewTransform || !this.fitTransform) return false;
    const minimum = this.fitTransform.scale * LOCAL_MAP_MIN_RELATIVE_ZOOM;
    const maximum = this.fitTransform.scale * LOCAL_MAP_MAX_RELATIVE_ZOOM;
    const oldScale = this.viewTransform.scale;
    const nextScale = Math.max(minimum, Math.min(maximum, oldScale * relativeFactor));
    const ratio = nextScale / oldScale;
    this.viewTransform.translateX = screenX - (screenX - this.viewTransform.translateX) * ratio;
    this.viewTransform.translateY = screenY - (screenY - this.viewTransform.translateY) * ratio;
    this.viewTransform.scale = nextScale;
    this.updateShapes();
    return true;
  }

  fitBounds() {
    if (!this.projectedGeometry) return false;
    const { width, height } = this.viewportSize();
    this.fitTransform = createLocalFitTransform(this.projectedGeometry, width, height);
    this.viewTransform = { ...this.fitTransform };
    this.svg?.setAttribute("viewBox", `0 0 ${width} ${height}`);
    this.updateShapes();
    return true;
  }

  attachInteractions() {
    const pointerDown = event => {
      event.preventDefault();
      this.svg?.setPointerCapture?.(event.pointerId);
      this.pointerPositions.set(event.pointerId, [event.clientX, event.clientY]);
      this.pinchDistance = null;
    };
    const pointerMove = event => {
      if (!this.pointerPositions.has(event.pointerId)) return;
      event.preventDefault();
      const previous = this.pointerPositions.get(event.pointerId);
      this.pointerPositions.set(event.pointerId, [event.clientX, event.clientY]);
      if (this.pointerPositions.size >= 2) {
        const [first, second] = [...this.pointerPositions.values()];
        const distance = Math.hypot(second[0] - first[0], second[1] - first[1]);
        const center = [(first[0] + second[0]) / 2, (first[1] + second[1]) / 2];
        if (this.pinchDistance > 0) this.zoomAt(distance / this.pinchDistance, center[0], center[1]);
        this.pinchDistance = distance;
      } else {
        this.panBy(event.clientX - previous[0], event.clientY - previous[1]);
      }
    };
    const pointerEnd = event => {
      this.pointerPositions.delete(event.pointerId);
      this.svg?.releasePointerCapture?.(event.pointerId);
      this.pinchDistance = null;
    };
    const wheel = event => {
      event.preventDefault();
      const rect = this.svg?.getBoundingClientRect?.() || { left: 0, top: 0 };
      this.zoomAt(Math.exp(-event.deltaY * 0.0015), event.clientX - rect.left, event.clientY - rect.top);
    };
    this.boundHandlers = { pointerdown: pointerDown, pointermove: pointerMove, pointerup: pointerEnd, pointercancel: pointerEnd, wheel };
    Object.entries(this.boundHandlers).forEach(([name, handler]) => this.svg.addEventListener(name, handler, name === "wheel" ? { passive: false } : undefined));
  }

  detachInteractions() {
    if (this.svg && this.boundHandlers) Object.entries(this.boundHandlers).forEach(([name, handler]) => this.svg.removeEventListener(name, handler));
    this.pointerPositions.clear();
    this.pinchDistance = null;
    this.boundHandlers = null;
  }

  async render(geometry) {
    this.detachInteractions();
    this.plan = createGeometryRenderPlan(geometry);
    this.projectedGeometry = projectWgs84GeometryForDisplay(structuredClone(geometry));
    const { width, height } = this.viewportSize();
    const svg = svgElement("svg", { viewBox: `0 0 ${width} ${height}`, role: "img", "aria-label": "当前地块地图", tabindex: "0" });
    svg.style.touchAction = "none";
    svg.append(svgElement("rect", { x: 0, y: 0, width, height, fill: "#edf2f6" }));
    this.shapes = geometryGroups(this.projectedGeometry).map(() => {
      const shape = svgElement(geometry.type === "Point" ? "circle" : geometry.type === "LineString" ? "polyline" : "polygon", {
        fill: geometry.type === "LineString" ? "none" : "#1976D2",
        "fill-opacity": geometry.type === "LineString" ? 0 : 0.15,
        stroke: "#E53935", "stroke-width": 3, "vector-effect": "non-scaling-stroke",
        ...(geometry.type === "Point" ? { r: 9 } : {})
      });
      svg.append(shape);
      return shape;
    });
    this.svg = svg;
    this.container.hidden = false;
    this.container.replaceChildren(svg);
    this.fitBounds();
    this.attachInteractions();
    if (this.attributionElement) {
      this.attributionElement.textContent = "";
      this.attributionElement.hidden = true;
    }
    return true;
  }

  getViewState() {
    return this.viewTransform ? Object.freeze({ ...this.viewTransform }) : null;
  }

  destroy() {
    this.detachInteractions();
    this.container?.replaceChildren();
    this.plan = null;
    this.svg = null;
    this.projectedGeometry = null;
    this.fitTransform = null;
    this.viewTransform = null;
    this.shapes = [];
  }
}

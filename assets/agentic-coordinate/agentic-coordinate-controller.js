import {
  acceptAgenticCoordinateRecognition,
  beginAgenticCoordinateUpload,
  bindAgenticCoordinateOutput,
  createAgenticCoordinateWorkspace,
  editAgenticCoordinateText,
  failAgenticCoordinateRecognition,
  getAgenticCoordinateDocument,
} from './agentic-coordinate-workspace.js';

export class AgenticCoordinateController {
  constructor({ fetchImpl = globalThis.fetch, onChange = () => {} } = {}) {
    if (typeof fetchImpl !== 'function') throw new Error('fetchImpl is required');
    this.fetchImpl = fetchImpl;
    this.onChange = onChange;
    this.enabled = false;
    this.workspace = createAgenticCoordinateWorkspace();
  }

  publish() {
    this.onChange(this.workspace);
    return this.workspace;
  }

  async initialize() {
    try {
      const response = await this.fetchImpl('/api/agentic-coordinate/v1/status', {
        cache: 'no-store',
      });
      const payload = await response.json().catch(() => ({}));
      this.enabled = response.ok && payload.enabled === true;
    } catch {
      this.enabled = false;
    }
    return this.enabled;
  }

  edit(text) {
    this.workspace = editAgenticCoordinateText(this.workspace, text);
    return this.publish();
  }

  async recognize(file) {
    if (!this.enabled) throw new Error('Agentic coordinate recognition is disabled');
    this.workspace = beginAgenticCoordinateUpload(this.workspace, { fileName: file?.name || '' });
    const uploadRevision = this.workspace.uploadRevision;
    this.publish();

    try {
      const form = new FormData();
      form.append('image', file);
      const response = await this.fetchImpl('/api/agentic-coordinate/v1/recognize', {
        method: 'POST',
        body: form,
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || payload.success !== true || !payload.result) {
        const error = new Error(payload.message || 'Coordinate recognition failed');
        error.code = payload.code || 'AGENTIC_COORDINATE_RECOGNITION_FAILED';
        throw error;
      }
      if (this.workspace.uploadRevision !== uploadRevision) return null;
      this.workspace = acceptAgenticCoordinateRecognition(this.workspace, payload.result);
      this.publish();
      return payload;
    } catch (error) {
      if (this.workspace.uploadRevision === uploadRevision) {
        this.workspace = failAgenticCoordinateRecognition(this.workspace, error);
        this.publish();
      }
      throw error;
    }
  }

  async finalize({ name = 'Coordinate result' } = {}) {
    if (!this.enabled) throw new Error('Agentic coordinate recognition is disabled');
    const document = getAgenticCoordinateDocument(this.workspace);
    if (!document.text.trim()) throw new Error('请输入或识别坐标后再继续。');

    const response = await this.fetchImpl('/api/agentic-coordinate/v1/finalize', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        documentRevision: document.documentRevision,
        currentText: document.text,
        sourceText: this.workspace.sourceText,
        recognitionResult: this.workspace.recognitionResult,
        name,
      }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || payload.success !== true) {
      const error = new Error(payload.message || 'Coordinate finalization failed');
      error.code = payload.code || 'AGENTIC_COORDINATE_FINALIZATION_FAILED';
      throw error;
    }

    this.workspace = bindAgenticCoordinateOutput(this.workspace, {
      documentRevision: payload.documentRevision,
      map: payload.map,
      kml: payload.kml,
    });
    this.publish();
    return payload;
  }
}


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
  constructor({ fetchImpl = globalThis.fetch, onChange = () => {}, usage = null } = {}) {
    if (typeof fetchImpl !== 'function') throw new Error('fetchImpl is required');
    this.fetchImpl = fetchImpl;
    this.onChange = onChange;
    this.usage = usage;
    this.enabled = false;
    this.recognitionUsageConsumed = false;
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

  hasCommittedRecognitionUsage() {
    return this.recognitionUsageConsumed === true;
  }

  usageIdentity() {
    const visitorId = String(this.usage?.getVisitorId?.() || '').trim();
    const requestId = String(this.usage?.createRequestId?.() || '').trim().toLowerCase();
    if (!visitorId || !requestId) throw new Error('安全请求编号初始化失败，请刷新页面后重试。');
    return { visitorId, requestId };
  }

  async acceptRecognitionPayload(payload, uploadRevision) {
    if (!payload?.result) throw new Error('Coordinate recognition returned no result');
    if (this.workspace.uploadRevision !== uploadRevision) return null;
    this.recognitionUsageConsumed = payload.usageConsumed === true;
    this.workspace = acceptAgenticCoordinateRecognition(this.workspace, payload.result);
    this.publish();
    return payload;
  }

  async recoverPending() {
    if (!this.enabled || typeof this.usage?.recover !== 'function') return null;
    const requestId = String(this.usage?.getPendingRequestId?.() || '').trim().toLowerCase();
    const visitorId = String(this.usage?.getVisitorId?.() || '').trim();
    if (!requestId || !visitorId) return null;

    this.workspace = beginAgenticCoordinateUpload(this.workspace, { fileName: 'recovered-result' });
    const uploadRevision = this.workspace.uploadRevision;
    this.publish();
    const recovered = await this.usage.recover({ visitorId, requestId });
    if (!recovered?.ok || recovered.payload?.success !== true || !recovered.payload?.result) {
      if (recovered?.payload?.usageConsumed === false || recovered?.payload?.recoveryTerminal === true) {
        this.usage?.clearPendingRequest?.();
      }
      const error = new Error(recovered?.payload?.message || 'Coordinate recognition recovery failed');
      error.code = recovered?.payload?.code || 'AGENTIC_COORDINATE_RECOVERY_FAILED';
      if (this.workspace.uploadRevision === uploadRevision) {
        this.workspace = failAgenticCoordinateRecognition(this.workspace, error);
        this.publish();
      }
      throw error;
    }
    this.usage?.clearPendingRequest?.();
    return this.acceptRecognitionPayload(recovered.payload, uploadRevision);
  }

  async recognize(file) {
    if (!this.enabled) throw new Error('Agentic coordinate recognition is disabled');
    this.recognitionUsageConsumed = false;
    this.workspace = beginAgenticCoordinateUpload(this.workspace, { fileName: file?.name || '' });
    const uploadRevision = this.workspace.uploadRevision;
    this.publish();

    try {
      const { visitorId, requestId } = this.usageIdentity();
      await this.usage?.prepareSession?.(visitorId);
      this.usage?.rememberPendingRequest?.(requestId);
      const form = new FormData();
      form.append('image', file);
      form.append('visitorId', visitorId);
      const response = await this.fetchImpl('/api/agentic-coordinate/v1/recognize', {
        method: 'POST',
        headers: this.usage?.headers?.({
          'x-visitor-id': visitorId,
          'x-recognition-request-id': requestId,
        }) || {
          'x-visitor-id': visitorId,
          'x-recognition-request-id': requestId,
        },
        body: form,
      });
      let payload = await response.json().catch(() => ({}));
      let finalResponseOk = response.ok;
      if (payload.code === 'USAGE_COMMIT_OUTCOME_UNKNOWN' && typeof this.usage?.recover === 'function') {
        const recovered = await this.usage.recover({ visitorId, requestId });
        payload = recovered?.payload || payload;
        finalResponseOk = recovered?.ok === true;
      }
      if (!finalResponseOk || payload.success !== true || !payload.result) {
        if (payload.usageConsumed === false || payload.recoveryTerminal === true) {
          this.usage?.clearPendingRequest?.();
        }
        const error = new Error(payload.message || 'Coordinate recognition failed');
        error.code = payload.code || 'AGENTIC_COORDINATE_RECOGNITION_FAILED';
        throw error;
      }
      this.usage?.clearPendingRequest?.();
      return this.acceptRecognitionPayload(payload, uploadRevision);
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

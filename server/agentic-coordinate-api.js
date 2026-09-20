import { normalizeAgenticCoordinateResult, runAgenticCoordinateRecognition } from './agentic-coordinate-recognition/index.js';
import { finalizeAgenticCoordinateDocument } from './agentic-coordinate-finalization/index.js';
import { buildAgenticCoordinateUsageAuthority } from './agentic-coordinate-usage-authority.js';

function apiErrorStatus(error) {
  if (error?.code === 'PROJECTED_CRS_TRANSFORM_REQUIRED'
    || error?.code === 'PROJECTED_TRANSFORM_FAILED') return 422;
  if (/must|invalid|unsupported|empty|required/i.test(String(error?.message || ''))) return 400;
  return 502;
}

function sendApiError(res, error) {
  return res.status(apiErrorStatus(error)).json({
    success: false,
    code: String(error?.code || 'AGENTIC_COORDINATE_FAILED'),
    message: String(error?.message || 'Agentic coordinate operation failed'),
  });
}

function imageDataUrl(file) {
  if (!file?.buffer || !Buffer.isBuffer(file.buffer)) {
    throw new Error('One image file is required');
  }
  const mimeType = String(file.mimetype || '').toLowerCase();
  if (!['image/jpeg', 'image/png'].includes(mimeType)) {
    throw new Error('Only JPEG or PNG coordinate images are supported');
  }
  return `data:${mimeType};base64,${file.buffer.toString('base64')}`;
}

export function createAgenticCoordinateApi({ modelName, providerCall }) {
  if (typeof providerCall !== 'function') throw new Error('providerCall is required');

  return Object.freeze({
    recognize: async (req, res) => {
      try {
        const outcome = await runAgenticCoordinateRecognition({
          imageDataUrl: imageDataUrl(req.file),
          modelName,
          providerCall,
        });
        if (!outcome.result.success) {
          res.setHeader('Cache-Control', 'no-store');
          return res.status(422).json({
            success: false,
            code: 'AGENTIC_COORDINATE_NOT_RECOGNIZED',
            message: '未能可靠识别坐标，请上传更清晰的坐标区域图片。',
          });
        }
        const recognitionRequestId = String(req.agenticRecognitionRequestId || '').trim().toLowerCase();
        const agenticCoordinateAuthority = buildAgenticCoordinateUsageAuthority({
          recognitionRequestId,
          result: outcome.result,
        });
        res.setHeader('Cache-Control', 'no-store');
        return res.json({
          success: true,
          requestId: recognitionRequestId,
          result: outcome.result,
          agenticCoordinateAuthority,
          execution: outcome.execution,
        });
      } catch (error) {
        return sendApiError(res, error);
      }
    },

    finalize: async (req, res) => {
      try {
        const recognitionResult = req.body?.recognitionResult
          ? normalizeAgenticCoordinateResult(req.body.recognitionResult)
          : null;
        const outcome = await finalizeAgenticCoordinateDocument({
          documentRevision: Number(req.body?.documentRevision),
          currentText: String(req.body?.currentText || ''),
          sourceText: String(req.body?.sourceText || ''),
          recognitionResult,
          name: String(req.body?.name || 'Coordinate result'),
          modelName,
          providerCall,
        });
        res.setHeader('Cache-Control', 'no-store');
        return res.json({
          success: true,
          documentRevision: outcome.documentRevision,
          result: outcome.result,
          spatialResult: outcome.spatialResult,
          map: outcome.map,
          kml: outcome.kml,
          execution: outcome.execution,
        });
      } catch (error) {
        return sendApiError(res, error);
      }
    },
  });
}

export function requireAgenticCoordinateApiEnabled(req, res, next) {
  const readiness = getAgenticCoordinateApiReadiness();
  if (!readiness.enabled) {
    return res.status(404).json({
      success: false,
      code: 'AGENTIC_COORDINATE_V1_DISABLED',
    });
  }
  return next();
}

export function getAgenticCoordinateApiReadiness() {
  const featureEnabled = String(process.env.AGENTIC_COORDINATE_V1_ENABLED || '').toLowerCase() === 'true';
  const atomicUsageReady = String(process.env.AGENTIC_COORDINATE_ATOMIC_USAGE_READY || '').toLowerCase() === 'true';
  return Object.freeze({
    enabled: featureEnabled && atomicUsageReady,
    featureEnabled,
    atomicUsageReady,
  });
}

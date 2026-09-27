import express from 'express';

export const COORDINATE_AGENT_NONPRODUCTION_ROUTE_PREFIX = '/api/nonproduction/coordinate-agent/v1';

function assertCaseRequest(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('A JSON request object is required');
  }
  const keys = Object.keys(value);
  if (keys.length !== 1 || keys[0] !== 'caseId') {
    throw new Error('The non-production route accepts only caseId');
  }
  const caseId = String(value.caseId || '').trim();
  if (!/^[a-z0-9][a-z0-9._-]{0,127}$/i.test(caseId)) throw new Error('caseId is invalid');
  return caseId;
}

export function createCoordinateAgentNonProductionRouter({ enabled = false, evaluateCase } = {}) {
  if (enabled !== true) throw new Error('Coordinate Agent non-production route is disabled');
  if (typeof evaluateCase !== 'function') throw new Error('evaluateCase is required');

  const router = express.Router();
  router.use(express.json({ limit: '32kb' }));
  router.post('/evaluate', async (req, res) => {
    try {
      const caseId = assertCaseRequest(req.body);
      const result = await evaluateCase({ caseId });
      res.status(200).json({
        schemaVersion: 'coordinate-agent-nonproduction-response/v1',
        caseId,
        result,
      });
    } catch (error) {
      res.status(400).json({
        schemaVersion: 'coordinate-agent-nonproduction-error/v1',
        error: String(error?.message || 'Non-production evaluation failed'),
      });
    }
  });
  return router;
}

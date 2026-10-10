export function createCoordinateThreeCaseRcJobForwarder({
  fetchImpl,
  getPort,
  internalToken,
  coordinateThreeCaseRcAdmission,
  indonesiaStructuredBRcAdmission,
  indonesiaProductMode
} = {}) {
  if (typeof fetchImpl !== "function" || typeof getPort !== "function") {
    throw new TypeError("coordinate_three_case_rc_forwarder_transport_required");
  }
  return async function executeCoordinateThreeCaseRcJob(input, { jobId } = {}) {
    const indonesiaStructuredRcJob = indonesiaStructuredBRcAdmission?.config?.ready === true
      && input.body?.coordinateProductMode === indonesiaProductMode;
    const terminateUnsettledClaim = async () => {
      if (!indonesiaStructuredRcJob) return;
      try {
        await indonesiaStructuredBRcAdmission.terminatePreProviderClaim({
          recognitionRequestId: input.requestId,
          authorization: input.forwardHeaders?.authorization
        });
      } catch {
        // A dispatched or already-settled claim remains governed by its authoritative terminal state.
      }
    };
    const terminateThreeCaseClaim = async () => {
      if (coordinateThreeCaseRcAdmission?.config?.guardRequired !== true) return;
      try {
        await coordinateThreeCaseRcAdmission.terminatePreProviderClaim({
          caseId: input.forwardHeaders?.["x-coordinate-rc-case-id"],
          productMode: input.body?.coordinateProductMode,
          recognitionRequestId: input.requestId,
          imageBuffer: input.file.buffer,
          authorization: input.forwardHeaders?.authorization
        });
      } catch {
        // A dispatched or already-settled claim remains governed by its authoritative terminal state.
      }
    };
    const form = new FormData();
    form.append("image", new Blob([input.file.buffer], { type: input.file.mimetype }), input.file.originalname);
    for (const [key, value] of Object.entries(input.body || {})) {
      if (typeof value === "string") form.append(key, value);
    }
    let response;
    try {
      response = await fetchImpl(`http://127.0.0.1:${getPort()}/api/internal/recognize-coordinates-long`, {
        method: "POST",
        headers: {
          ...input.forwardHeaders,
          "x-recognition-async-internal-token": internalToken,
          "x-recognition-job-id": jobId
        },
        body: form
      });
    } catch (error) {
      await terminateUnsettledClaim();
      await terminateThreeCaseClaim();
      throw error;
    }
    const result = await response.json().catch(() => ({
      success: false,
      reason: "async_response_invalid",
      rawText: "",
      coordinates: ""
    }));
    if (!response.ok) {
      await terminateUnsettledClaim();
      await terminateThreeCaseClaim();
    }
    return { httpStatus: response.status, result };
  };
}

export function createCoordinateThreeCaseRcInternalBindingMiddleware({
  admission,
  activateDeadlineContext
} = {}) {
  if (!admission || typeof activateDeadlineContext !== "function") {
    throw new TypeError("coordinate_three_case_rc_internal_binding_dependencies_required");
  }
  return async function bindCoordinateThreeCaseRcInternalBudget(req, res, next) {
    req.coordinateThreeCaseRcAsyncAuthorized = true;
    if (admission.config.guardRequired !== true) return next();
    const deadlineContext = activateDeadlineContext(req);
    const budget = deadlineContext?.budget || null;
    try {
      admission.bindBudget({
        budget,
        caseId: req.get("x-coordinate-rc-case-id"),
        productMode: req.body?.coordinateProductMode,
        recognitionRequestId: req.get("x-recognition-request-id"),
        imageBuffer: req.file?.buffer,
        authorization: req.get("authorization")
      });
      return next();
    } catch (error) {
      await admission.terminatePreProviderClaim({
        caseId: req.get("x-coordinate-rc-case-id"),
        productMode: req.body?.coordinateProductMode,
        recognitionRequestId: req.get("x-recognition-request-id"),
        imageBuffer: req.file?.buffer,
        authorization: req.get("authorization")
      }).catch(() => {});
      return res.status(error?.httpStatus || 503).json({
        success: false,
        reason: error?.code || "COORDINATE_THREE_CASE_RC_BINDING_FAILED"
      });
    }
  };
}

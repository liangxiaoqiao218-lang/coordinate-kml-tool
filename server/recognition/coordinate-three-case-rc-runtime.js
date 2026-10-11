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
          authorization: input.forwardHeaders?.authorization,
          visitorId: input.forwardHeaders?.["x-visitor-id"]
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

export function createCoordinateThreeCaseRcJobIngressMiddleware({ admission } = {}) {
  if (!admission || typeof admission.validateVisitorEnvelope !== "function") {
    throw new TypeError("coordinate_three_case_rc_ingress_admission_required");
  }
  return function validateCoordinateThreeCaseRcJobVisitor(req, res, next) {
    if (admission.config.guardRequired !== true) return next();
    try {
      const binding = admission.validateVisitorEnvelope({
        headerVisitorId: req.get("x-visitor-id"),
        bodyVisitorId: req.body?.visitorId,
        authorization: req.get("authorization")
      });
      req.coordinateThreeCaseRcVisitorId = binding.visitorId;
      return next();
    } catch (error) {
      return res.status(error?.httpStatus || 422).json({
        success: false,
        reason: "coordinate_three_case_rc_visitor_rejected",
        code: error?.code || "COORDINATE_THREE_CASE_RC_VISITOR_REJECTED"
      });
    }
  };
}

export function createCoordinateThreeCaseRcJobPreflightMiddleware({
  admission,
  isRecognitionRequestId,
  randomRequestId
} = {}) {
  if (!admission || typeof admission.preflightJob !== "function"
    || typeof isRecognitionRequestId !== "function" || typeof randomRequestId !== "function") {
    throw new TypeError("coordinate_three_case_rc_job_preflight_dependencies_required");
  }
  return function preflightCoordinateThreeCaseRcJob(req, res, next) {
    if (admission.config.guardRequired !== true) return next();
    if (!req.file?.buffer) {
      return res.status(400).json({ success: false, reason: "image_required" });
    }
    const headerMode = String(req.get("x-coordinate-product-mode") || "").trim();
    const bodyMode = String(req.body?.coordinateProductMode || "").trim();
    if (headerMode && bodyMode && headerMode !== bodyMode) return next();
    const suppliedRequestId = String(req.get("x-recognition-request-id") || "").trim().toLowerCase();
    const recognitionRequestId = isRecognitionRequestId(suppliedRequestId)
      ? suppliedRequestId
      : randomRequestId();
    const productMode = headerMode || bodyMode;
    const caseId = String(req.get("x-coordinate-rc-case-id") || "").trim().toLowerCase();
    const originalImageBuffer = Buffer.from(req.file.buffer);
    try {
      admission.preflightJob({
        caseId,
        productMode,
        recognitionRequestId,
        imageBuffer: originalImageBuffer,
        authorization: req.get("authorization"),
        visitorId: req.coordinateThreeCaseRcVisitorId
      });
      req.coordinateThreeCaseRcJobPreflight = Object.freeze({
        caseId,
        productMode,
        recognitionRequestId,
        originalImageBuffer
      });
      return next();
    } catch (error) {
      return res.status(error?.httpStatus || 422).json({
        success: false,
        reason: error?.code || "COORDINATE_THREE_CASE_RC_JOB_PREFLIGHT_REJECTED"
      });
    }
  };
}

export function evaluateCoordinateThreeCaseRcPermissionGate({
  guardRequired = false,
  visitorId = "",
  user = null,
  permissions = null
} = {}) {
  if (!String(visitorId || "").trim()) {
    return Object.freeze({
      allowed: false,
      httpStatus: 400,
      body: Object.freeze({
        success: false,
        reason: "missing_user",
        code: "COORDINATE_VISITOR_ID_REQUIRED",
        error: "缺少用户信息，请刷新页面后重试。",
        rawText: "",
        coordinates: ""
      })
    });
  }
  if (guardRequired && !user) {
    return Object.freeze({
      allowed: false,
      httpStatus: 403,
      body: Object.freeze({
        success: false,
        reason: "rc_visitor_not_authorized",
        code: "COORDINATE_THREE_CASE_RC_VISITOR_NOT_AUTHORIZED",
        error: "当前测试访客未获本次受控识别权限。",
        rawText: "",
        coordinates: ""
      })
    });
  }
  if (permissions?.aiOcrEnabled !== true) {
    return Object.freeze({
      allowed: false,
      httpStatus: 403,
      body: Object.freeze({
        success: false,
        reason: "ai_ocr_permission_denied",
        code: "COORDINATE_AI_OCR_PERMISSION_DENIED",
        error: "当前用户暂未开通图片识别。",
        rawText: "",
        coordinates: ""
      })
    });
  }
  return Object.freeze({ allowed: true, httpStatus: 200, body: null });
}

export function createCoordinateThreeCaseRcExistingUserLookup({ supabase } = {}) {
  return async function findExistingCoordinateThreeCaseRcUser(visitorId) {
    if (!supabase || !String(visitorId || "").trim()) return null;
    const { data, error } = await supabase
      .from("users")
      .select("user_id")
      .eq("user_id", String(visitorId).trim())
      .maybeSingle();
    if (error) throw error;
    return data || null;
  };
}

export async function executeCoordinateThreeCaseRcPermissionBoundary({
  guardRequired = false,
  visitorId = "",
  readAdminData,
  findExistingUser,
  ensureUser,
  getEffectivePermissions,
  onAuthorized
} = {}) {
  if (typeof readAdminData !== "function" || typeof ensureUser !== "function"
    || typeof getEffectivePermissions !== "function" || typeof onAuthorized !== "function"
    || (guardRequired && typeof findExistingUser !== "function")) {
    throw new TypeError("coordinate_three_case_rc_permission_boundary_dependencies_required");
  }
  const adminData = await readAdminData();
  const localUser = ensureUser(adminData, visitorId);
  const sourceUser = guardRequired ? await findExistingUser(visitorId) : localUser;
  const user = guardRequired && !sourceUser ? null : localUser;
  const permissions = getEffectivePermissions(localUser, adminData?.featureFlags || {});
  const gate = evaluateCoordinateThreeCaseRcPermissionGate({
    guardRequired,
    visitorId,
    user,
    permissions
  });
  if (!gate.allowed) {
    return Object.freeze({
      allowed: false,
      httpStatus: gate.httpStatus,
      body: gate.body,
      adminData,
      user,
      permissions
    });
  }
  const value = await onAuthorized(Object.freeze({ adminData, user, permissions }));
  return Object.freeze({
    allowed: true,
    httpStatus: 200,
    body: null,
    adminData,
    user,
    permissions,
    value
  });
}

export async function settleCoordinateThreeCaseRcPermissionFailure({
  admission,
  budget,
  usageCommitController,
  httpStatus,
  body
} = {}) {
  if (!admission || typeof admission.settleForBudget !== "function"
    || !usageCommitController || typeof usageCommitController.settle !== "function") {
    throw new TypeError("coordinate_three_case_rc_permission_settlement_dependencies_required");
  }
  await admission.settleForBudget({ budget, httpStatus, body });
  const usageSettlement = await usageCommitController.settle({ httpStatus, body });
  if (usageSettlement?.kind !== "UNCHARGED_RESPONSE") {
    const error = new Error("coordinate_three_case_rc_permission_response_not_uncharged");
    error.code = "COORDINATE_THREE_CASE_RC_PERMISSION_RESPONSE_NOT_UNCHARGED";
    throw error;
  }
  return Object.freeze({
    ...body,
    usageConsumed: false,
    userUsageConsumed: false,
    recoveryRequired: false
  });
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
      const visitorBinding = admission.validateVisitorEnvelope({
        headerVisitorId: req.get("x-visitor-id"),
        bodyVisitorId: req.body?.visitorId,
        authorization: req.get("authorization")
      });
      admission.bindBudget({
        budget,
        caseId: req.get("x-coordinate-rc-case-id"),
        productMode: req.body?.coordinateProductMode,
        recognitionRequestId: req.get("x-recognition-request-id"),
        imageBuffer: req.file?.buffer,
        authorization: req.get("authorization"),
        visitorId: visitorBinding.visitorId
      });
      return next();
    } catch (error) {
      await admission.terminatePreProviderClaim({
        caseId: req.get("x-coordinate-rc-case-id"),
        productMode: req.body?.coordinateProductMode,
        recognitionRequestId: req.get("x-recognition-request-id"),
        imageBuffer: req.file?.buffer,
        authorization: req.get("authorization"),
        visitorId: req.get("x-visitor-id") || req.body?.visitorId
      }).catch(() => {});
      return res.status(error?.httpStatus || 503).json({
        success: false,
        reason: error?.code || "COORDINATE_THREE_CASE_RC_BINDING_FAILED"
      });
    }
  };
}

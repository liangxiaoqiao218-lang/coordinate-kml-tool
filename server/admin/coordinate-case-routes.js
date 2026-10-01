function statusFor(error) {
  return String(error?.code || "").startsWith("COORDINATE_CASE_") ? 400 : 500;
}

export function registerAdminCoordinateCaseRoutes(app, {
  requireAdmin,
  requireStore,
  store,
  contract,
  getModelStatus
}) {
  app.get("/api/admin/coordinate-cases", requireAdmin, async (req, res) => {
    try {
      if (!requireStore(res)) return;
      const result = await store.list({
        limit: req.query.limit,
        progressStatus: req.query.progress_status,
        issueType: req.query.issue_type
      });
      res.json({ success: true, contract, ...result });
    } catch (error) {
      console.error("Read coordinate cases failed:", error);
      res.status(500).json({ success: false, error: error.message || "读取坐标案例失败" });
    }
  });

  app.post("/api/admin/coordinate-cases", requireAdmin, async (req, res) => {
    try {
      if (!requireStore(res)) return;
      res.status(201).json({ success: true, case: await store.create(req.body || {}) });
    } catch (error) {
      console.error("Create coordinate case failed:", error?.code || error);
      res.status(statusFor(error)).json({ success: false, error: error.message || "创建坐标案例失败" });
    }
  });

  app.patch("/api/admin/coordinate-cases/:caseId", requireAdmin, async (req, res) => {
    try {
      if (!requireStore(res)) return;
      res.json({ success: true, case: await store.update(req.params.caseId, req.body || {}) });
    } catch (error) {
      console.error("Update coordinate case failed:", error?.code || error);
      res.status(statusFor(error)).json({ success: false, error: error.message || "更新坐标案例失败" });
    }
  });

  app.post("/api/admin/coordinate-cases/:caseId/evidence", requireAdmin, async (req, res) => {
    try {
      if (!requireStore(res)) return;
      res.status(201).json({ success: true, evidence: await store.addEvidence(req.params.caseId, req.body || {}) });
    } catch (error) {
      console.error("Create coordinate case evidence failed:", error?.code || error);
      res.status(statusFor(error)).json({ success: false, error: error.message || "创建坐标案例证据失败" });
    }
  });

  app.get("/api/admin/model-status", requireAdmin, (req, res) => {
    res.setHeader("Cache-Control", "no-store");
    res.json({ success: true, ...getModelStatus() });
  });
}

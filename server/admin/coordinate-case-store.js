import { buildCoverageSummary, normalizeCoordinateCaseEvidenceInput, normalizeCoordinateCaseInput, publicCoordinateCase } from "./coordinate-case-contract.js";

const CASE_FIELDS = "case_id,case_number,recognition_request_id,job_id,result_id,result_revision,geometry_hash,result_identity_status,issue_type,issue_summary,resolution_summary,owner,progress_status,blocker,delivery_status,problem_resolution_status,peer_validation_status,production_status,evidence_scope,sample_count,crs_evidence_status,geometry_representation,original_artifact_status,golden_ref,commit_sha,receipt_ref,created_at,updated_at";
const EVIDENCE_FIELDS = "evidence_id,case_id,evidence_type,status,evidence_scope,sample_count,reference_type,reference_value,created_at";

function isMissingTable(error) {
  return error?.code === "42P01" || error?.code === "PGRST205";
}

export class CoordinateCaseStore {
  constructor({ supabase }) {
    this.supabase = supabase;
  }

  async validateIdentity(input) {
    if (!input.result_id) return "REQUEST_ONLY";
    const { data, error } = await this.supabase.rpc("admin_validate_coordinate_case_identity", {
      p_recognition_request_id: input.recognition_request_id,
      p_result_id: input.result_id,
      p_result_revision: input.result_revision,
      p_geometry_hash: input.geometry_hash
    });
    if (error) throw error;
    const status = String(data || "UNKNOWN").toUpperCase();
    if (status !== "CURRENT") {
      throw Object.assign(new Error(`COORDINATE_CASE_RESULT_IDENTITY_${status}`), { code: "COORDINATE_CASE_RESULT_IDENTITY_CONFLICT", identityStatus: status });
    }
    return status;
  }

  async list({ limit = 100, progressStatus = "", issueType = "" } = {}) {
    let query = this.supabase.from("coordinate_cases").select(CASE_FIELDS).order("created_at", { ascending: false }).limit(Math.min(Math.max(Number(limit) || 100, 1), 500));
    if (progressStatus) query = query.eq("progress_status", String(progressStatus).toUpperCase());
    if (issueType) query = query.eq("issue_type", String(issueType).toLowerCase());
    const { data, error } = await query;
    if (error) {
      if (isMissingTable(error)) return { setupRequired: true, cases: [], coverage: buildCoverageSummary([]) };
      throw error;
    }
    const rows = data || [];
    let evidence = [];
    if (rows.length) {
      const result = await this.supabase.from("coordinate_case_evidence").select(EVIDENCE_FIELDS).in("case_id", rows.map(row => row.case_id)).order("created_at", { ascending: false });
      if (result.error && !isMissingTable(result.error)) throw result.error;
      evidence = result.data || [];
    }
    const byCase = new Map();
    for (const item of evidence) {
      const key = String(item.case_id);
      if (!byCase.has(key)) byCase.set(key, []);
      byCase.get(key).push(item);
    }
    const cases = rows.map(row => publicCoordinateCase(row, byCase.get(String(row.case_id)) || []));
    return { setupRequired: false, cases, coverage: buildCoverageSummary(cases) };
  }

  async create(raw) {
    const payload = normalizeCoordinateCaseInput(raw);
    payload.result_identity_status = await this.validateIdentity(payload);
    const { data, error } = await this.supabase.from("coordinate_cases").insert(payload).select(CASE_FIELDS).single();
    if (error) throw error;
    return publicCoordinateCase(data, []);
  }

  async update(caseId, raw) {
    const currentResult = await this.supabase.from("coordinate_cases").select(CASE_FIELDS).eq("case_id", String(caseId)).single();
    if (currentResult.error) throw currentResult.error;
    const payload = normalizeCoordinateCaseInput({ ...currentResult.data, ...raw });
    payload.result_identity_status = await this.validateIdentity(payload);
    const { data, error } = await this.supabase.from("coordinate_cases").update({ ...payload, updated_at: new Date().toISOString() }).eq("case_id", String(caseId)).select(CASE_FIELDS).single();
    if (error) throw error;
    return publicCoordinateCase(data, []);
  }

  async addEvidence(caseId, raw) {
    const payload = { ...normalizeCoordinateCaseEvidenceInput(raw), case_id: String(caseId) };
    const { data, error } = await this.supabase.from("coordinate_case_evidence").insert(payload).select(EVIDENCE_FIELDS).single();
    if (error) throw error;
    return publicCoordinateCase({}, [data]).evidence[0];
  }
}

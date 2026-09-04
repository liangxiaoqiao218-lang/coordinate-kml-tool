import { BILLING_PRODUCT_SCHEMA_VERSION, PAYMENT_PURCHASE_TYPES } from "./contracts.js";
import { PAYMENT_ERROR_CODES, paymentContractAssert } from "./errors.js";
import { freezePaymentContract, paymentAmountMinor, paymentCurrency, paymentIdentifier } from "./schemas.js";

export const PRODUCT_CLASS = Object.freeze({
  FREE: "free",
  PROFESSIONAL: "professional",
  ENTERPRISE: "enterprise",
  QUOTA_PACKAGE: "quota_package"
});

export const PRODUCT_PURCHASE_POLICY = Object.freeze({
  free: Object.freeze({ paidOrderAllowed: false, purchaseType: null, selfCheckoutEnabled: false, salesFlow: "free_default" }),
  professional: Object.freeze({ paidOrderAllowed: true, purchaseType: "subscription", selfCheckoutEnabled: true, salesFlow: "provider_checkout" }),
  enterprise: Object.freeze({ paidOrderAllowed: false, purchaseType: null, selfCheckoutEnabled: false, salesFlow: "manual_enterprise_sales" }),
  quota_package: Object.freeze({ paidOrderAllowed: true, purchaseType: "quota_package", selfCheckoutEnabled: true, salesFlow: "provider_checkout" })
});

export const FEATURE_NAME_MAPPINGS = Object.freeze([
  Object.freeze({
    domain: "coordinate",
    legacyName: "coordinate_analysis",
    productionNames: Object.freeze(["coordinate", "convert"]),
    canonicalBillingName: "coordinate_processing",
    mappingStatus: "EXPLICIT_CONTRACT_ONLY",
    changesProductionAuthority: false
  }),
  Object.freeze({
    domain: "mining",
    legacyName: null,
    productionNames: Object.freeze(["mining", "judge"]),
    canonicalBillingName: "mining_judgement",
    mappingStatus: "LEGACY_UNMAPPED",
    changesProductionAuthority: false
  }),
  Object.freeze({
    domain: "report",
    legacyName: "report_generation",
    productionNames: Object.freeze([]),
    canonicalBillingName: "report_generation",
    mappingStatus: "PRODUCTION_UNMAPPED",
    changesProductionAuthority: false
  })
]);

export function resolveProductPurchasePolicy(productClass) {
  const policy = PRODUCT_PURCHASE_POLICY[String(productClass ?? "").trim().toLowerCase()];
  paymentContractAssert(policy, PAYMENT_ERROR_CODES.PRODUCT_NOT_FOUND, "unknown product class");
  return policy;
}

export function createBillingProductContract(value = {}) {
  const productClass = String(value.productClass ?? "").trim().toLowerCase();
  const policy = resolveProductPurchasePolicy(productClass);
  paymentContractAssert(policy.paidOrderAllowed, PAYMENT_ERROR_CODES.INVALID_REQUEST, "product class cannot create a paid billing product");
  const purchaseType = String(value.purchaseType ?? policy.purchaseType).trim();
  paymentContractAssert(PAYMENT_PURCHASE_TYPES.includes(purchaseType), PAYMENT_ERROR_CODES.INVALID_REQUEST, "invalid product purchaseType");
  paymentContractAssert(purchaseType === policy.purchaseType, PAYMENT_ERROR_CODES.INVALID_REQUEST, "product purchaseType conflicts with product policy");
  paymentContractAssert(value.grantDefinition && typeof value.grantDefinition === "object", PAYMENT_ERROR_CODES.INVALID_REQUEST, "grantDefinition required");
  return freezePaymentContract({
    schemaVersion: BILLING_PRODUCT_SCHEMA_VERSION,
    productId: paymentIdentifier(value.productId, "productId"),
    productClass,
    purchaseType,
    priceMinor: paymentAmountMinor(value.priceMinor, "priceMinor"),
    currency: paymentCurrency(value.currency),
    grantDefinition: structuredClone(value.grantDefinition),
    status: ["active", "inactive"].includes(value.status) ? value.status : "active",
    version: paymentIdentifier(value.version, "version"),
    paidOrderAllowed: true,
    selfCheckoutEnabled: policy.selfCheckoutEnabled
  });
}

export function createProductSnapshot(product) {
  paymentContractAssert(product?.schemaVersion === BILLING_PRODUCT_SCHEMA_VERSION, PAYMENT_ERROR_CODES.PRODUCT_NOT_FOUND, "billing product required");
  return freezePaymentContract(structuredClone(product));
}

export function resolveFeatureNameMapping(name, namespace) {
  const value = String(name ?? "").trim();
  const mapping = FEATURE_NAME_MAPPINGS.find(item => {
    if (namespace === "legacy") return item.legacyName === value;
    if (namespace === "production") return item.productionNames.includes(value);
    if (namespace === "canonical") return item.canonicalBillingName === value;
    return false;
  });
  paymentContractAssert(mapping, PAYMENT_ERROR_CODES.PRODUCT_NOT_FOUND, "feature name mapping not found");
  return mapping;
}

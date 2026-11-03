import { PAYMENT_ERROR_CODES, paymentContractAssert } from "./errors.js";
import {
  assertPaymentOrderContract, assertVerifiedPaymentEvent, assertVerifiedProviderEvidence,
  createTrustedStoredPaymentOrderReader, freezePaymentContract, paymentIdentifier
} from "./schemas.js";
import { assertPaymentStateApplication } from "./state-machine.js";

const boundaries = new WeakSet();
const methods = ["loadOrder", "confirmPaidOnce", "loadGrantTask", "applyGrantOnce"];

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  return JSON.stringify(value);
}

// The driver is a trusted server dependency. Implementations must provide the
// two database transactions described in the contract. No driver is supplied
// here; interface shape and a committed receipt are not database proof.
export function createPaymentPersistence({ driver = null } = {}) {
  const configured = driver !== null && methods.every(method => typeof driver[method] === "function");
  function requireConfigured() {
    paymentContractAssert(configured, PAYMENT_ERROR_CODES.PROVIDER_UNAVAILABLE, "payment persistence not configured");
  }
  const reader = createTrustedStoredPaymentOrderReader({ readOrder: async id => {
    requireConfigured();
    return driver.loadOrder(id);
  } });
  const boundary = Object.freeze({
    isConfigured: () => configured,
    async loadOrder(orderId) { requireConfigured(); return reader.load(orderId); },
    async confirmPaidOnce({ order, verifiedEvidence, paymentEvent, stateApplication = null, billingEvent = null } = {}) {
      requireConfigured();
      assertPaymentOrderContract(order);
      const evidence = assertVerifiedProviderEvidence(verifiedEvidence);
      const event = assertVerifiedPaymentEvent(paymentEvent);
      paymentContractAssert(order.provider === evidence.provider && order.orderId === evidence.orderId &&
        order.amountMinor === evidence.amountMinor && order.currency === evidence.currency &&
        evidence.paymentStateEvidence === "PAID" && event.orderId === order.orderId &&
        event.providerOrderId === evidence.providerOrderId && event.providerEventId === evidence.providerEventId &&
        event.eventType === "PAYMENT_CONFIRMED",
      PAYMENT_ERROR_CODES.ORDER_MISMATCH, "confirmation command provenance mismatch");
      if (order.status !== "PAID") {
        const application = assertPaymentStateApplication(stateApplication);
        paymentContractAssert(application.to === "PAID" && application.order.orderId === order.orderId &&
          application.providerEvidenceReference?.providerEventId === evidence.providerEventId &&
          billingEvent?.orderId === order.orderId && billingEvent?.userId === order.userId &&
          stableJson(billingEvent.grantDefinition) === stableJson(order.productSnapshot.grantDefinition),
        PAYMENT_ERROR_CODES.CONFIRMED_PROVENANCE_INVALID, "state-applied billing intent required");
      }
      const command = freezePaymentContract({
        expectedOrder: structuredClone(order),
        evidence: structuredClone(evidence),
        event: structuredClone(event),
        eventKey: `${order.provider}:${order.appId}:${evidence.providerEventId}`,
        nextOrder: stateApplication ? structuredClone(stateApplication.order) : null,
        billingEvent: billingEvent ? structuredClone(billingEvent) : null,
        grantKey: order.orderId
      });
      const receipt = await driver.confirmPaidOnce(command);
      paymentContractAssert(receipt?.committed === true && typeof receipt.firstPaid === "boolean" &&
        receipt.orderId === order.orderId && receipt.providerOrderId === evidence.providerOrderId,
      PAYMENT_ERROR_CODES.STATE_APPLICATION_REQUIRED, "T1 committed confirmation receipt required");
      const paidOrder = await reader.load(order.orderId);
      paymentContractAssert(paidOrder?.status === "PAID" && paidOrder.userId === order.userId &&
        paidOrder.providerOrderId === evidence.providerOrderId && paidOrder.amountMinor === order.amountMinor &&
        stableJson(paidOrder.productSnapshot) === stableJson(order.productSnapshot),
      PAYMENT_ERROR_CODES.STATE_APPLICATION_REQUIRED, "committed PAID order not confirmed by store");
      return Object.freeze({ order: paidOrder, firstPaid: receipt.firstPaid });
    },
    async applyGrantOnce(order) {
      requireConfigured();
      assertPaymentOrderContract(order);
      paymentContractAssert(order.status === "PAID", PAYMENT_ERROR_CODES.INVALID_STATE_TRANSITION, "grant requires PAID order");
      const task = await driver.loadGrantTask(order.orderId);
      paymentContractAssert(task?.orderId === order.orderId && task.key === order.orderId && task.userId === order.userId &&
        ["PENDING", "FAILED", "APPLIED"].includes(task.status) &&
        Number.isSafeInteger(task.version) && task.version >= 0 &&
        stableJson(task.grantDefinition) === stableJson(order.productSnapshot.grantDefinition),
      PAYMENT_ERROR_CODES.CONFIRMED_PROVENANCE_INVALID, "original persisted grant task required");
      if (task.status === "APPLIED") return freezePaymentContract(structuredClone(task));
      const receipt = await driver.applyGrantOnce(freezePaymentContract({
        key: task.key, orderId: order.orderId, userId: order.userId,
        expectedTaskVersion: task.version, grantDefinition: structuredClone(order.productSnapshot.grantDefinition)
      }));
      paymentContractAssert(receipt?.key === order.orderId && ["PENDING", "FAILED", "APPLIED"].includes(receipt.status),
        PAYMENT_ERROR_CODES.CONFIRMED_PROVENANCE_INVALID, "grant transaction receipt invalid");
      return freezePaymentContract(structuredClone(receipt));
    },
    requireConfigured
  });
  boundaries.add(boundary);
  return boundary;
}

export function assertPaymentPersistence(boundary) {
  paymentContractAssert(boundaries.has(boundary), PAYMENT_ERROR_CODES.INVALID_REQUEST, "configured server persistence boundary required");
  return boundary;
}

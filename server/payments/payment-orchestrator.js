import { PAYMENT_ERROR_CODES, paymentContractAssert } from "./errors.js";
import {
  assertVerifiedPaymentIdentity,
  createServerVerifiedPaymentIdentity,
  createPaymentEventContract,
  paymentIdentifier
} from "./schemas.js";
import { applyPaymentStateTransition, createPaymentConfirmedBillingEvent } from "./state-machine.js";
import { assertPaymentPersistence } from "./payment-persistence.js";

function requireMethod(target, method, owner) {
  paymentContractAssert(typeof target?.[method] === "function", PAYMENT_ERROR_CODES.INVALID_REQUEST, `${owner}.${method} required`);
}

export class PaymentConfirmationOrchestrator {
  constructor({ adapter, orderStore, transactionStore, entitlementStore, persistence = null } = {}) {
    paymentContractAssert(adapter?.provider === "alipay", PAYMENT_ERROR_CODES.PROVIDER_MISMATCH, "Alipay adapter required");
    requireMethod(adapter, "verifyAndParseWebhook", "adapter");
    requireMethod(adapter, "queryPayment", "adapter");
    requireMethod(adapter, "createEvidence", "adapter");
    if (persistence !== null) {
      this.persistence = assertPaymentPersistence(persistence);
    } else {
      for (const method of ["get", "save", "withOrderLock"]) requireMethod(orderStore, method, "orderStore");
      for (const method of ["claim"]) requireMethod(transactionStore, method, "transactionStore");
      for (const method of ["applyOnce", "get"]) requireMethod(entitlementStore, method, "entitlementStore");
      this.persistence = null;
    }
    this.adapter = adapter;
    this.orderStore = orderStore;
    this.transactionStore = transactionStore;
    this.entitlementStore = entitlementStore;
  }

  async confirmNotification({ identity, envelope } = {}) {
    paymentContractAssert(this.persistence === null, PAYMENT_ERROR_CODES.INVALID_REQUEST, "legacy entry requires legacy stores");
    const verifiedIdentity = assertVerifiedPaymentIdentity(identity);
    const record = await this.adapter.verifyAndParseWebhook(envelope);
    return this.#confirm({ identity: verifiedIdentity, record });
  }

  async confirmQuery({ identity, orderId } = {}) {
    paymentContractAssert(this.persistence === null, PAYMENT_ERROR_CODES.INVALID_REQUEST, "legacy entry requires legacy stores");
    const verifiedIdentity = assertVerifiedPaymentIdentity(identity);
    const normalizedOrderId = paymentIdentifier(orderId, "orderId");
    const order = await this.orderStore.get(normalizedOrderId);
    paymentContractAssert(order?.userId === verifiedIdentity.userId, PAYMENT_ERROR_CODES.ORDER_OWNERSHIP_MISMATCH, "payment identity does not own order");
    const record = await this.adapter.queryPayment({ order });
    return this.#confirm({ identity: verifiedIdentity, record });
  }

  // Server-to-server entry. Ownership comes from the trusted original order,
  // after actual verification; no browser session or caller identity is read.
  async confirmProviderNotification({ envelope } = {}) {
    const persistence = assertPaymentPersistence(this.persistence);
    persistence.requireConfigured();
    const record = await this.adapter.verifyAndParseWebhook(envelope);
    const order = await persistence.loadOrder(record.merchantOrderId);
    paymentContractAssert(order && record.appId === order.appId && record.sellerId === order.sellerId,
      PAYMENT_ERROR_CODES.ORDER_MISMATCH, "original order merchant binding mismatch");
    const identity = createServerVerifiedPaymentIdentity({ userId: order.userId, verification: "trusted_service" });
    const evidence = this.adapter.createEvidence({ record, order, identity });
    if (evidence.paymentStateEvidence !== "PAID") {
      return Object.freeze({ acknowledge: true, paid: false, firstPaid: false, entitlementStatus: "NOT_REQUESTED" });
    }
    const event = createPaymentEventContract({ eventId: record.providerEventId, eventType: "PAYMENT_CONFIRMED",
      verifiedEvidence: evidence, payloadHash: record.payloadHash, receivedAt: record.observedAt });
    let application = null;
    let billingEvent = null;
    if (order.status !== "PAID") {
      let pending = order;
      if (pending.status === "CREATED") pending = applyPaymentStateTransition({ order: pending, to: "PENDING",
        appliedAt: record.observedAt, transitionId: `${record.providerEventId}:pending` }).order;
      application = applyPaymentStateTransition({ order: pending, to: "PAID", appliedAt: record.observedAt,
        transitionId: `${record.providerEventId}:paid`, verifiedEvidence: evidence });
      billingEvent = createPaymentConfirmedBillingEvent({ eventId: `billing:${record.providerEventId}`, identity,
        order: application.order, verifiedEvidence: evidence, paymentEvent: event,
        stateApplication: application, createdAt: record.observedAt });
    }
    const confirmed = await persistence.confirmPaidOnce({ order, verifiedEvidence: evidence, paymentEvent: event,
      stateApplication: application, billingEvent });
    const grant = await this.#applyPersistentGrant(confirmed.order);
    return Object.freeze({ acknowledge: true, paid: true, firstPaid: confirmed.firstPaid,
      order: confirmed.order, entitlementStatus: grant.status, entitlementReceipt: grant });
  }

  // An internal service operation; this batch does not expose a recovery route.
  async recoverPendingGrant({ orderId } = {}) {
    const persistence = assertPaymentPersistence(this.persistence);
    persistence.requireConfigured();
    const order = await persistence.loadOrder(paymentIdentifier(orderId, "orderId"));
    paymentContractAssert(order?.status === "PAID", PAYMENT_ERROR_CODES.INVALID_STATE_TRANSITION, "recovery requires stored PAID order");
    const grant = await this.#applyPersistentGrant(order);
    return Object.freeze({ paid: true, order, entitlementStatus: grant.status, entitlementReceipt: grant });
  }

  async #applyPersistentGrant(order) {
    try { return await this.persistence.applyGrantOnce(order); }
    catch (_) {
      // T1 already committed a unique pending task. A driver error can mean an
      // uncertain T2 outcome; never turn PAID back to unpaid or retry here.
      return Object.freeze({ key: order.orderId, status: "PENDING", reason: "GRANT_OUTCOME_UNCONFIRMED" });
    }
  }

  async #confirm({ identity, record }) {
    return this.orderStore.withOrderLock(record.merchantOrderId, async () => {
      let order = await this.orderStore.get(record.merchantOrderId);
      paymentContractAssert(order?.schemaVersion === "payment_order_v1", PAYMENT_ERROR_CODES.INVALID_REQUEST, "payment order not found");
      paymentContractAssert(order.userId === identity.userId, PAYMENT_ERROR_CODES.ORDER_OWNERSHIP_MISMATCH, "payment identity does not own order");
      const evidence = this.adapter.createEvidence({ record, order, identity });

      if (evidence.paymentStateEvidence !== "PAID") {
        return Object.freeze({ paid: false, firstPaid: false, order, entitlementStatus: "NOT_REQUESTED" });
      }

      const transactionClaim = await this.transactionStore.claim({
        provider: "alipay",
        providerTransactionId: record.providerTransactionId,
        orderId: order.orderId
      });
      paymentContractAssert(
        transactionClaim?.claimed === true || transactionClaim?.orderId === order.orderId,
        PAYMENT_ERROR_CODES.ORDER_MISMATCH,
        "provider transaction belongs to another order"
      );

      if (order.status === "PAID") {
        paymentContractAssert(order.providerOrderId === record.providerTransactionId, PAYMENT_ERROR_CODES.ORDER_MISMATCH, "paid order transaction mismatch");
        const existingEntitlement = await this.entitlementStore.get(order.orderId);
        return Object.freeze({
          paid: true,
          firstPaid: false,
          order,
          entitlementStatus: existingEntitlement?.status || "PENDING",
          entitlementReceipt: existingEntitlement || null
        });
      }

      if (order.status === "CREATED") {
        const pending = applyPaymentStateTransition({
          order,
          to: "PENDING",
          appliedAt: record.observedAt,
          transitionId: `${record.providerEventId}:pending`
        });
        order = pending.order;
        await this.orderStore.save(order);
      }
      paymentContractAssert(order.status === "PENDING", PAYMENT_ERROR_CODES.INVALID_STATE_TRANSITION, "order cannot enter PAID from current state");

      const paidApplication = applyPaymentStateTransition({
        order,
        to: "PAID",
        appliedAt: record.observedAt,
        transitionId: `${record.providerEventId}:paid`,
        verifiedEvidence: evidence
      });
      const paidOrder = paidApplication.order;
      await this.orderStore.save(paidOrder);

      const paymentEvent = createPaymentEventContract({
        eventId: record.providerEventId,
        eventType: "PAYMENT_CONFIRMED",
        verifiedEvidence: evidence,
        payloadHash: record.payloadHash,
        receivedAt: record.observedAt
      });
      const billingEvent = createPaymentConfirmedBillingEvent({
        eventId: `billing:${record.providerEventId}`,
        identity,
        order: paidOrder,
        verifiedEvidence: evidence,
        paymentEvent,
        stateApplication: paidApplication,
        createdAt: record.observedAt
      });

      try {
        const entitlementReceipt = await this.entitlementStore.applyOnce({ key: paidOrder.orderId, billingEvent });
        return Object.freeze({
          paid: true,
          firstPaid: true,
          order: paidOrder,
          entitlementStatus: entitlementReceipt?.status || "PENDING",
          entitlementReceipt: entitlementReceipt || null
        });
      } catch (error) {
        const failedReceipt = typeof this.entitlementStore.markFailed === "function"
          ? await this.entitlementStore.markFailed({ key: paidOrder.orderId, billingEvent, error })
          : Object.freeze({ key: paidOrder.orderId, status: "FAILED" });
        return Object.freeze({
          paid: true,
          firstPaid: true,
          order: paidOrder,
          entitlementStatus: failedReceipt?.status === "PENDING" ? "PENDING" : "FAILED",
          entitlementReceipt: failedReceipt || null
        });
      }
    });
  }
}

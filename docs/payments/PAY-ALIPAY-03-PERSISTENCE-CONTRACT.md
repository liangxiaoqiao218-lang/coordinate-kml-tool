# PAY-ALIPAY-03-A persistence integration contract

Base: 8f2e804074788cdd8144b5ad4014b7ee55644cac. This batch adds local notification handling and transaction interfaces only. It installs no route, driver, scheduler, merchant configuration, migration, identity mapping or production balance changes.

## Existing source and exact missing connections

All paths below are relative to the sole inspected worktree:
`C:/Users/Mir-1/Documents/Codex/2026-10-28/geokitlab-pay-alipay-01-offline`.

| Boundary | Existing capability to reuse | Missing implementation / next owner |
|---|---|---|
| Raw notification HTTP ingress | `server.js` Express application | III assigns one shared-file owner; route-specific raw form bytes and dedicated request limits before inappropriate body/auth/origin middleware; no route mounted here |
| Cryptographic verification | `server/payments/alipay-rsa2-verifier.js` | Trusted merchant public key / certificate selection and official payload compatibility remain unqualified; no real key loaded here |
| Original order resolution | `server/payments/alipay-notification-decoder.js` resolver injection | Actual payment-order table, server-created authenticated owner and fixed app/seller/product snapshot; no existing durable payment order store found |
| Trusted persisted-order reader | `server/payments/schemas.js` existing order validators and WeakSet provenance | New fixed-reader factory validates store rows; live store must be composed by trusted server code, never a request-controlled callback; UUID syntax is not user authentication |
| T1 `loadOrder` / `confirmPaidOnce` | Payment Core states/events and injected stores in `server/payments/payment-orchestrator.js` | No existing database implementation. The old get/save/claim sequence and memory lock cannot implement this transaction. Add a single database function/RPC or a real SQL transaction driver in a separate batch |
| T2 `loadGrantTask` / `applyGrantOnce` | `server.js` existing `users.paid_convert_count` / `paid_judge_count` and usage log paths | No existing atomic grant RPC or payment receipt table. Admin add-count and consumeUsage use read-then-write replacement and must not be reused as atomic payment grants |
| Balance owner | PAY-ID confirmed authenticated UUID; legacy usage uses visitorId / users.user_id | III establishes authoritative mapping and unique ownership. Never assume string equality, auto-create a mapping from a notification, migrate, merge or convert balances |
| Failed grant recovery | New explicit internal `recoverPendingGrant` operation | Actual task storage, recovery operator/job and durable retry policy are absent; no recovery HTTP route or background process started |

`payment-persistence.js` accepts a trusted driver with exactly four methods. Missing methods or no driver fail closed. A driver's `committed` receipt is an interface assertion, not independently verified database durability. This batch supplies only a synthetic driver in its test script.

## Ownership and authority

Notification handling does not require browser cookies, JWTs or a signed-in client. It first performs actual verification with the configured trusted key, then resolves the merchant order and obtains ownership from that trusted store. The original order must have been created under verified server authentication in the future actual order-creation path. This batch creates no such public order route.

Do not derive ownership from buyer_id, visitorId, pass-through data or a JSON verified flag. The legacy browser-authenticated notification/query APIs remain available only with the old injected stores. The new persistence entry has a distinct server-to-server method. No public method turns arbitrary request JSON into a trusted order: the store reader fixes its callback at server composition, validates schema, UUID, snapshot/amount/currency/status/version and merchant binding, and rebuilds only CREATED/PENDING/PAID orders. Refunded states are outside this batch.

The database adapter must validate the same immutable facts again under its row lock; an earlier JavaScript read is not sufficient. All command fields remain untrusted to SQL unless checked against the locked original order and trusted service permissions.

## Candidate durable constraints (not an applied schema)

- Unique original merchant order identifier; authenticated UUID, product/price/grant snapshot, application and seller fixed at creation.
- Unique `(provider, app_id, provider_event_id)` with a payload/critical-content digest. Same event identifier with changed payload is a conflict, never a fresh grant.
- Unique `(provider, provider_transaction_id)` mapped to one original order.
- One first-purchase grant task/receipt key per order. This receipt is an idempotency and audit record, not another spendable balance pool.
- Explicit authoritative UUID-to-existing-balance-account mapping. Its real table, column types and foreign keys remain undecided; names in commands are interface properties, not claimed database columns.
- Scope transaction functions to the intended server role; examine actual table ownership, grants and RLS before deployment. Do not grant browser clients payment-state or balance mutation. Prefer invoker permissions; do not add SECURITY DEFINER merely to bypass an error.

No existing payment constraints have been verified in a live database. This contract intentionally is not an executable migration. Generate a real migration only in an authorized schema batch after inspecting the target database and the current supported tooling.

## T1: confirm payment once

`confirmPaidOnce(command)` must execute one database transaction:

1. Lock the original order, recheck owner, app, seller, amount, currency, product snapshot, provider, transaction and admissible state/version.
2. Enforce event uniqueness and reject changed content under the same event key.
3. Enforce cross-order transaction uniqueness.
4. For the first payment, persist the centralized Core PAID transition and the immutable billing/grant intent, with one PENDING task.
5. For a duplicate, recheck its transaction, PAID state and existing original task; return firstPaid=false without new balance changes. A version read before concurrent confirmation must not prohibit a genuine duplicate once the immutable facts are rechecked.
6. Commit all effects together. Return committed=true only after that commit. The boundary then reloads the trusted PAID order.

Before durable confirmation, errors yield failure/503 to the handler, never a success acknowledgment. If commit happened but its acknowledgment was lost, the outcome is uncertain to the caller: do not manufacture an unpaid state or create a new order. Read the durable order/task and use the original keys on retry/recovery.

## T2: grant once

`applyGrantOnce(command)` must execute one database transaction. Lock the task/receipt and the mapped existing balance account, recheck original PAID order and original grant snapshot, and check the unique grant key. APPLIED is a no-op. Missing mapping returns PENDING/MAPPING_REQUIRED and changes no balance. Never invent or infer an account mapping.

For an eligible task, increment the existing approved paid field(s) using database arithmetic, then mark the unique task/receipt APPLIED in the same transaction. A failure rolls back both. The first-batch synthetic fixture uses a numeric model of one existing account; it does not authorize a new production balance or assume the old judge product is still sold.

The new orchestrator does not automatically retry uncertain T2 calls. It preserves PAID and returns PENDING/GRANT_OUTCOME_UNCONFIRMED. The original durable task remains recoverable. Internal recovery loads the original task and compares owner/grant snapshot; APPLIED returns without another increment. A lost post-commit acknowledgment therefore must not cause duplicate credit.

The HTTP handler may acknowledge success after T1's committed confirmation even if the task is still PENDING. Fulfillment then belongs to durable recovery, not to repeated provider notifications alone. T1 acknowledgment loss continues to return failure; retries are allowed only using the same original event/order/transaction keys.

## Consumption / admin / refund boundary

`server.js` legacy consumption and admin adjustment paths currently read a balance and send its replacement value. Updating only payment credit would still permit lost updates. III must assign their single owner to adopt compatible database arithmetic/transactions against the same mapped account. Payment, consumption and future refund handling require consistent ownership and concurrency semantics.

Retain old data, balances and logs. Do not revive the terminated arbitrary-image mining sales route. Current three-quota product directions do not establish a third database field in this older base. Do not freeze a guessed field or migrate balances. Refund policy, non-renewing plan semantics and any actual SKU/price configuration remain separate decisions.

## Evidence and limits

One new script, at most 28 scenarios, one Node process, no old test groups. Tests use real in-memory RSA2 and existing decoder/adapter/Core, with synthetic storage and authentication context. The fixture serializes in-process clone/swap operations to exercise failures and interface order. A copied fixture state is not an operating-system restart or durable persistence. Modeled concurrent grants/consumption do not prove the real legacy consumer is safe.

Actual database uniqueness, rollback, locks, concurrent sessions/processes, recovery after real restarts, row permissions and consumption interleavings need a separately approved isolated database batch. Official notification compatibility, trusted merchant keys/certificates and real callbacks/query behavior need a later merchant-environment batch. Offline PASS is not production payment acceptance.

## III coordination checklist / next actual integration

1. Decide canonical authenticated UUID and its authoritative mapping to the existing balance account. Preserve unmapped status; do not auto-migrate or convert old entitlements.
2. Assign exactly one owner for the eventual server.js route mount, authentication composition and consumption/admin transaction changes; establish the shared integration commit rather than editing parallel product branches.
3. Name the next isolated database environment and authorize schema inspection, candidate constraints/RPC implementation and bounded multi-session transaction tests separately. No such environment is selected here.

After read-only review of this batch, proceed to those actual storage and route connections. Do not spend successive batches inventing more abstract payment contracts.

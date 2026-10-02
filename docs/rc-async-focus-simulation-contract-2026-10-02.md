# RC asynchronous input-protection simulation contract

This test surface exists only to qualify coordinate-input behavior on the existing non-production Render RC service. It is not a recognition path and does not create a product result.

## Activation boundary

The server publishes the capability only when all of these conditions are true:

- `ENABLE_RC_ASYNC_FOCUS_SIMULATION=true`
- `DEPLOYMENT_TIER=rc`
- `RENDER_SERVICE_NAME=coordinate-kml-tool-rc`
- runtime branch is `hotfix/production-generic-dms-review-recovery`

The browser additionally requires the current URL to contain `rc-async-focus-test=1`. Without both the server gate and this session URL, the control remains hidden. No browser storage is used to retain activation.

## Test behavior

Starting the test captures the current coordinate-input revision and current finalized-result identity, then waits six seconds. During the wait the tester edits the coordinate input and keeps it focused. When the delayed event arrives, the test checks that:

- the input revision changed and the stale result was rejected by the same revision gate used by recognition;
- the tester's new input and focus remain unchanged;
- the active finalized `resultId`, `resultRevision`, and `geometryHash` remain unchanged;
- a deliberately conflicting simulated identity is rejected.

The delayed event never writes input or result data. It does not upload a file, call a Provider or map service, create a recognition job, consume usage, write a database, or persist test data.

## Qualification limits

Desktop browser automation proves the gate and behavior contract but does not prove an iPhone soft keyboard or system back gesture. Those remain separate true-device checks on the exact RC deployment. This test surface must not be enabled on the production service or included as evidence that recognition, map accuracy, KML, or formal authorization passed.

// One sequential run only: retain bounded tails and exact exit/timeout results.
import { spawn } from 'node:child_process';
import { mkdirSync, writeFileSync, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
const root = fileURLToPath(new URL('../', import.meta.url));
const phaseC = process.argv.includes('--phase-c');
const projectedD1 = process.argv.includes('--d1-projected');
const projectedTestD1 = process.argv.includes('--d1-projected-test');
const projectedMarkdownD1 = process.argv.includes('--d1-projected-markdown');
const projectedHttpIdentityD1 = process.argv.includes('--d1-projected-http-identity');
const projectedHttpNegativesD1 = process.argv.includes('--d1-projected-http-negatives');
const projectedExtraFieldD1 = process.argv.includes('--d1-projected-extra-field');
const resumeD1Direction = process.argv.includes('--resume-d1-direction');
const integrityD1 = process.argv.includes('--d1-integrity') || resumeD1Direction;
const resumeD1HistoricalBaseline = process.argv.includes('--resume-d1-historical-baseline') || integrityD1;
const resumeD1Markdown = process.argv.includes('--resume-d1-markdown');
const downstreamD1 = process.argv.includes('--d1-downstream') || resumeD1Markdown || resumeD1HistoricalBaseline;
const resumeD1State = process.argv.includes('--resume-d1-state') || downstreamD1;
const resumeD1Http = process.argv.includes('--resume-d1-http') || resumeD1State;
const phaseD1 = process.argv.includes('--phase-d1') || resumeD1Http || projectedD1 || projectedTestD1 || projectedMarkdownD1 || projectedHttpIdentityD1 || projectedHttpNegativesD1 || projectedExtraFieldD1;
const resumePhaseBHttp = process.argv.includes('--resume-phase-b-http');
if (phaseC && (resumePhaseBHttp || process.argv.includes('--phase-b'))) throw new Error('ONE_PHASE_PER_RUN');
if (phaseD1 && (phaseC || resumePhaseBHttp || process.argv.includes('--phase-b'))) throw new Error('ONE_PHASE_PER_RUN');
const phaseB = process.argv.includes('--phase-b') || resumePhaseBHttp || phaseC || phaseD1;
const output = path.join(root, 'Temp', projectedExtraFieldD1 ? 'recognition-table-phase-d1-projected-extra-field-recovery' : projectedHttpNegativesD1 ? 'recognition-table-phase-d1-projected-http-negatives-recovery' : projectedHttpIdentityD1 ? 'recognition-table-phase-d1-projected-http-identity-recovery' : projectedMarkdownD1 ? 'recognition-table-phase-d1-projected-markdown-recovery' : projectedTestD1 ? 'recognition-table-phase-d1-projected-test-recovery' : projectedD1 ? 'recognition-table-phase-d1-projected-recovery' : resumeD1Direction ? 'recognition-table-phase-d1-direction-recovery' : integrityD1 ? 'recognition-table-phase-d1-integrity-recovery' : resumeD1HistoricalBaseline ? 'recognition-table-phase-d1-historical-baseline-recovery' : resumeD1Markdown ? 'recognition-table-phase-d1-markdown-recovery' : downstreamD1 ? 'recognition-table-phase-d1-downstream' : resumeD1State ? 'recognition-table-phase-d1-http-state-recovery' : resumeD1Http ? 'recognition-table-phase-d1-http-recovery' : phaseD1 ? 'recognition-table-phase-d1' : phaseC ? 'recognition-result-phase-c' : resumePhaseBHttp ? 'recognition-diagnostics-phase-b-http-recovery'
  : phaseB ? 'recognition-diagnostics-phase-b' : 'recognition-architecture-audit');
mkdirSync(output, { recursive: true });
const resultPath = path.join(output, 'results.json');
if (existsSync(resultPath)) throw new Error('Existing audit run: do not automatically rerun');
const env = { ...process.env };
for (const name of Object.keys(env)) {
  if (/PROVIDER|SUPABASE|ALIYUN|DASHSCOPE|OPENAI|ANTHROPIC|GEMINI|API_KEY|API_TOKEN|SECRET|USAGE|PASSWORD/i.test(name)) env[name] = '';
}
Object.assign(env, { NODE_ENV: 'test', DOTENV_CONFIG_PATH: '__no_audit_environment_file__',
  NODE_OPTIONS: `--require=${JSON.stringify(path.join(root, 'scripts', 'recognition-audit-offline-guard.cjs'))}` });
if (downstreamD1 || projectedD1 || projectedTestD1 || projectedMarkdownD1 || projectedHttpIdentityD1 || projectedHttpNegativesD1 || projectedExtraFieldD1) env.RECOGNITION_AUDIT_RECEIPT_ROOT = output;
const plannedSteps = [
  ...(phaseD1 ? [
    ...['server/recognition/recognition-table-input.js', 'server/recognition/recognition-first-acquisition.js',
      'scripts/recognition-table-input-regression.js', 'scripts/recognition-diagnostics-http-regression.js']
      .map(file => [`syntax:${file}`, ['--check', file]]),
    ['recognition-table-input-regression', ['scripts/recognition-table-input-regression.js']],
    ['recognition-table-input-http-regression', ['scripts/recognition-diagnostics-http-regression.js', resumeD1Direction ? '--resume-d1-direction' : resumeD1HistoricalBaseline ? '--resume-d1-historical-baseline' : downstreamD1 ? '--d1-downstream' : resumeD1State ? '--resume-d1-state' : resumeD1Http ? '--resume-d1-http' : '--phase-d1', ...(integrityD1 ? ['--d1-integrity'] : [])]],
    ['phase-c-historical-shadow-regression', ['scripts/coordinate-result-shadow-regression.js', '--d1-historical']]
  ] : []),
  ...(phaseC ? [
    ['syntax:coordinate-result-shadow', ['--check', 'scripts/coordinate-result-shadow.js']],
    ['syntax:coordinate-result-shadow-regression', ['--check', 'scripts/coordinate-result-shadow-regression.js']],
    ['coordinate-result-shadow-regression', ['scripts/coordinate-result-shadow-regression.js']]
  ] : []),
  ['syntax', ['--check', 'server.js']],
  ...(phaseB ? ['server/recognition/recognition-diagnostics.js', 'server/recognition/recognition-first-acquisition.js',
    'scripts/recognition-diagnostic-replay.js', 'scripts/recognition-diagnostics-regression.js',
    'scripts/recognition-diagnostics-http-regression.js'].map(file => [`syntax:${file}`, ['--check', file]]) : []),
  ...[
    ...(phaseB ? ['recognition-diagnostics-regression', 'recognition-diagnostics-http-regression']
      : ['recognition-architecture-cleanup-regression']),
    'projected-crs-source-evidence-regression',
    'multi-representation-source-evidence-regression',
    'coordinate-markdown-table-regression',
    'recognition-first-review-result-v2-regression',
    'recognition-first-acquisition-evidence-v3-regression',
    'multi-representation-http-regression',
    'p08h-confirmation-ui-lifecycle-regression',
    'source-coordinate-review-display-regression',
    'review-output-contract-regression',
    'recognition-projected-authorization-v8-regression',
    'production-recognition-recovery-p0-regression',
    'production-core-capability-closure-p0-regression'
  ].map(name => [name, [`scripts/${name}.js`, ...(phaseD1 && name === 'recognition-diagnostics-regression' ? ['--d1-historical'] : [])]])
];
// Explicitly authorized continuation: do not rerun successful Phase B checks.
const steps = projectedExtraFieldD1
  ? [
    ...['server/recognition/recognition-candidate-evidence.js','server/recognition/recognition-table-input.js',
      'scripts/recognition-projected-extra-field-regression.js','scripts/recognition-projected-integrity-http-regression.js']
      .map(file => [`syntax:${file}`,['--check',file]]),
    ['recognition-projected-extra-field-regression',['scripts/recognition-projected-extra-field-regression.js']],
    ['recognition-projected-integrity-http-regression',['scripts/recognition-projected-integrity-http-regression.js','--negative-only','--from-extra']],
    ['recognition-integrity-output-regression',['scripts/recognition-integrity-output-regression.js']],
    ['recognition-table-input-regression',['scripts/recognition-table-input-regression.js']],
    ['recognition-acquisition-downstream-regression',['scripts/recognition-acquisition-downstream-regression.js']],
    ['recognition-table-input-http-regression',['scripts/recognition-diagnostics-http-regression.js','--d1-downstream','--d1-integrity']],
    ['phase-c-historical-shadow-regression',['scripts/coordinate-result-shadow-regression.js','--d1-historical']],
    ...plannedSteps.slice(plannedSteps.findIndex(([name]) => name === 'syntax'))
  ]
  : projectedHttpNegativesD1
  ? [
    ['syntax:scripts/recognition-projected-integrity-http-regression.js',['--check','scripts/recognition-projected-integrity-http-regression.js']],
    ['recognition-projected-integrity-http-regression',['scripts/recognition-projected-integrity-http-regression.js','--negative-only']],
    ['recognition-integrity-output-regression',['scripts/recognition-integrity-output-regression.js']],
    ['recognition-table-input-regression',['scripts/recognition-table-input-regression.js']],
    ['recognition-acquisition-downstream-regression',['scripts/recognition-acquisition-downstream-regression.js']],
    ['recognition-table-input-http-regression',['scripts/recognition-diagnostics-http-regression.js','--d1-downstream','--d1-integrity']],
    ['phase-c-historical-shadow-regression',['scripts/coordinate-result-shadow-regression.js','--d1-historical']],
    ...plannedSteps.slice(plannedSteps.findIndex(([name]) => name === 'syntax'))
  ]
  : projectedHttpIdentityD1
  ? [
    ['syntax:scripts/recognition-projected-integrity-http-regression.js',['--check','scripts/recognition-projected-integrity-http-regression.js']],
    ['recognition-projected-integrity-http-regression',['scripts/recognition-projected-integrity-http-regression.js']],
    ['recognition-integrity-output-regression',['scripts/recognition-integrity-output-regression.js']],
    ['recognition-table-input-regression',['scripts/recognition-table-input-regression.js']],
    ['recognition-acquisition-downstream-regression',['scripts/recognition-acquisition-downstream-regression.js']],
    ['recognition-table-input-http-regression',['scripts/recognition-diagnostics-http-regression.js','--d1-downstream','--d1-integrity']],
    ['phase-c-historical-shadow-regression',['scripts/coordinate-result-shadow-regression.js','--d1-historical']],
    ...plannedSteps.slice(plannedSteps.findIndex(([name]) => name === 'syntax'))
  ]
  : (projectedD1 || projectedTestD1 || projectedMarkdownD1)
  ? [
    ...['server/evidence-acquisition/local-ocr-map-layout-classifier.js', 'server/recognition/recognition-candidate-evidence.js',
      'scripts/recognition-projected-pair-regression.js', 'scripts/recognition-projected-integrity-http-regression.js',
      'scripts/recognition-table-input-regression.js'].map(file => [`syntax:${file}`,['--check',file]]),
    ['recognition-projected-pair-regression',['scripts/recognition-projected-pair-regression.js']],
    ['recognition-projected-integrity-http-regression',['scripts/recognition-projected-integrity-http-regression.js']],
    ['recognition-integrity-output-regression',['scripts/recognition-integrity-output-regression.js']],
    ['recognition-table-input-regression',['scripts/recognition-table-input-regression.js']],
    ['recognition-acquisition-downstream-regression',['scripts/recognition-acquisition-downstream-regression.js']],
    ['recognition-table-input-http-regression',['scripts/recognition-diagnostics-http-regression.js','--d1-downstream','--d1-integrity']],
    ['phase-c-historical-shadow-regression',['scripts/coordinate-result-shadow-regression.js','--d1-historical']],
    ...plannedSteps.slice(plannedSteps.findIndex(([name]) => name === 'syntax'))]
  : resumeD1Direction
  ? plannedSteps.slice(plannedSteps.findIndex(([name]) => name === 'recognition-table-input-http-regression'))
  : integrityD1
  ? [['recognition-integrity-output-regression', ['scripts/recognition-integrity-output-regression.js']],
    ...plannedSteps.slice(plannedSteps.findIndex(([name]) => name === 'recognition-table-input-http-regression'))]
  : resumeD1HistoricalBaseline
  ? plannedSteps.slice(plannedSteps.findIndex(([name]) => name === 'recognition-table-input-http-regression'))
  : downstreamD1
  ? [['recognition-acquisition-downstream-regression', ['scripts/recognition-acquisition-downstream-regression.js',
    ...(resumeD1Markdown ? ['--resume-markdown'] : [])]],
    ...plannedSteps.slice(plannedSteps.findIndex(([name]) => name === 'recognition-table-input-http-regression'))]
  : resumeD1Http
  ? plannedSteps.slice(plannedSteps.findIndex(([name]) => name === 'recognition-table-input-http-regression'))
  : resumePhaseBHttp
  ? plannedSteps.slice(plannedSteps.findIndex(([name]) => name === 'recognition-diagnostics-http-regression'))
  : plannedSteps;
const results = [];
for (const [name, args] of steps) {
  console.log(`START ${name}`);
  const started = Date.now();
  const record = await new Promise(resolve => {
    const stepEnv = (projectedD1 || projectedTestD1 || projectedMarkdownD1 || projectedHttpIdentityD1 || projectedHttpNegativesD1 || projectedExtraFieldD1) && ['recognition-projected-integrity-http-regression','recognition-table-input-http-regression'].includes(name)
      ? {...env, RECOGNITION_AUDIT_RECEIPT_ROOT:path.join(output,name)} : env;
    const child = spawn(process.execPath, args, { cwd: root, env:stepEnv, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
    let tail = '', timedOut = false;
    const capture = chunk => { tail = (tail + chunk.toString()).slice(-20000); };
    child.stdout.on('data', capture); child.stderr.on('data', capture);
    const timer = setTimeout(() => { timedOut = true; child.kill(); }, 600000);
    child.on('error', error => { tail += `\n${error.message}`; });
    child.on('close', (exitCode, signal) => { clearTimeout(timer); resolve({ name, exitCode, signal, timedOut,
      elapsedMs: Date.now() - started, tail }); });
  });
  results.push(record);
  writeFileSync(resultPath, JSON.stringify({ baseline: '86ea4ac44a6d44a2332d7aaaffb1926504a73dca',
    phase: phaseD1 ? 'D1' : phaseC ? 'C' : phaseB ? 'B' : 'A',
    recovery: projectedExtraFieldD1 ? 'projected-extra-field-full-consumption' : projectedHttpNegativesD1 ? 'projected-http-negative-map-status' : projectedHttpIdentityD1 ? 'projected-http-identity-source' : projectedMarkdownD1 ? 'projected-markdown-fixture' : projectedTestD1 ? 'projected-test-input-construction' : projectedD1 ? 'complete-projected-serialization-integrity' : resumeD1Direction ? 'historical-direction-preview-classification' : integrityD1 ? 'integrity-output-gate' : resumeD1HistoricalBaseline ? 'historical-point-preview-classification' : resumeD1Markdown ? 'markdown-field-binding' : downstreamD1 ? 'candidate-downstream-consumption' : resumeD1State ? 'http-final-state-classification' : resumeD1Http ? 'json-http-differential-baseline' : resumePhaseBHttp ? 'handler-scope-http-capture' : null,
    runScope: 'offline only; external sockets refused; no production requests', results }, null, 2));
  console.log(`END ${name}: exit=${record.exitCode} timeout=${record.timedOut} durationMs=${record.elapsedMs}`);
  console.log(record.tail.split(/\r?\n/).slice(-4).join('\n'));
  if (record.exitCode !== 0 || record.timedOut) { process.exitCode = 1; break; }
}
console.log(`Result receipt: ${resultPath}`);

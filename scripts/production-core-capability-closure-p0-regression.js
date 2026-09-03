import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {fileURLToPath} from 'node:url';
import vm from 'node:vm';
import * as routing from '../server/recognition/family-primary-routing.js';
import * as boundary from '../server/structured-coordinate-boundary.js';
import * as finalizer from '../server/coordinate-finalizer/index.js';
import {convertKyrgyzGkToWgs84} from '../server/projection/kyrgyz-gk.js';
import {FinalizedResultSpatialGeometryAdapter} from '../server/spatial/adapters/finalized-result-adapter.js';
import {MapPreviewAdapter} from '../server/spatial/adapters/map-preview-adapter.js';

const source = await readFile(new URL('../server.js', import.meta.url), 'utf8');
const html = await readFile(new URL('../index.html', import.meta.url), 'utf8');
const oldP0 = await readFile(new URL('./production-recognition-recovery-p0-regression.js', import.meta.url), 'utf8');
// Reuse exact observed acquisition, without upgrading its values to Golden truth.
const handwritten = oldP0.match(/const text = `(11°28'37[\s\S]*?)`;/)[1].replace(/\r\n/g, '\n');
assert.equal(createHash('sha256').update(handwritten).digest('hex'), '495a43fcb5659a274fb3357fad12e95f7c792550f26daf8cfff6b41c18626444');
const rows = handwritten.split('\n').filter(line => line.includes('°'));
const cleanPrinted = rows.join('\n');
const replay = JSON.parse(await readFile(new URL('../release-governance/p0-deterministic-replay-manifest.json', import.meta.url), 'utf8'));
const projected = replay.records[0].approvedAcquisitionLines.join('\n');
const unresolved = projected.replace('UTM WGS 1984 ZONA 50S', '');
const kyrgyz = 'Координаты угловых точек | № points | X | Y\n3 | 13261350 | 4607780\n1 | 13261341 | 4607777\n2 | 13261345 | 4607778';

if (process.argv[2] === '--http') {
  const http = await import('node:http');
  let calls = 0;
  const scenario = process.argv[3];
  if (scenario === 'manual') {
    for (const [name, patch] of Object.entries({missing:{}, authority:{explicitAuthorityRejected:true}, crs:{crs:null}, transform:{kmlAuthorityBlocked:true}, rejected:{confirmationStatus:'rejected'}, v3:{sourceAuthority:'coordinate_engine_v3'}})) {
      finalizer.registerFinalizedCoordinateResult(finalizer.finalizeCoordinateResult({
        resultId:'core-recovery-'+name,resultRevision:1,currentRevision:1,sourceAuthority:'legacy',
        coordinateType:'handwritten_dms_experimental',crs:finalizer.FINALIZED_COORDINATE_CRS,
        geometry:null,qualityGateStatus:'review_required',confirmationStatus:'pending',requiresReview:true,kmlReady:false,...patch
      }));
    }
  }
  globalThis.fetch = async url => {
    assert.equal(String(url), 'http://127.0.0.1:1/v1/chat/completions');
    assert.ok(++calls <= 3, 'unexpected retry expansion');
    const content = scenario === 'kyrgyz' ? kyrgyz : scenario === 'unresolved' ? unresolved : handwritten;
    return new Response(JSON.stringify({choices:[{message:{content}}]}), {status:200,headers:{'content-type':'application/json'}});
  };
  const listen = http.Server.prototype.listen;
  http.Server.prototype.listen = function(port, cb) {
    this.once('listening', () => process.send({port:this.address().port}));
    return listen.call(this,port,'127.0.0.1',cb);
  };
  await import('../server.js');
  await new Promise(() => {});
}

function extract(text, name) {
  const start = text.search(new RegExp(`(?:async )?function ${name}\\(`));
  assert.ok(start >= 0, name);
  const tail = text.slice(start);
  for (const end of tail.matchAll(/^\s*\}/gm)) {
    const code = tail.slice(0,end.index + end[0].length);
    try { new vm.Script(code); return code; } catch { /* nested/template brace */ }
  }
  throw new Error('function extraction failed: '+name);
}
const runtime = vm.createContext({...routing,...boundary,Buffer});
for (const match of source.matchAll(/^(?:async )?function (\w+)\(/gm)) vm.runInContext(extract(source,match[1]),runtime);
for (const name of ['noCoordinatesText','MGRS_BANDS','MGRS_COLUMN_SETS','MGRS_ROW_SETS','MOZAMBIQUE_TETE_KNOWN_ROW_TOLERANCE']) {
  vm.runInContext(source.match(new RegExp(`^const ${name} = .+;$`,'m'))[0],runtime);
}
const browser = vm.createContext({Number,Array,activeFinalizedCoordinateResult:null});
vm.runInContext(extract(html,'getFinalizedCoordinateIdentity'),browser);
vm.runInContext(extract(html,'getCanonicalCoordinateDisplayText'),browser);
const tests=[];
const test=(name,fn)=>tests.push({name,fn});
const adapter=new FinalizedResultSpatialGeometryAdapter();
const points=rows.map((row,i)=>boundary.parseStructuredBoundaryPoint(row,'handwritten_dms_experimental',i));
function engine(type='handwritten_dms_experimental', pts=points) {
  return {coordinate_type:type,precision_mode:type==='handwritten_dms_experimental'?'handwritten-dms-coordinates':'dms-coordinates',requires_review:true,
    groups:[{group_id:'g1',geometry:pts.length===1?'point':'polygon',requires_review:true,kml_ready:false,points:pts}],warnings:['Review warning']};
}
function make({structured=engine(),recognition={},verification={status:'REVIEW',warnings:['Review warning']},revision={},availability=null}={}) {
  const input=finalizer.createLegacyFinalizerInput({coordinateEngineV2:structured,recognitionResult:recognition,verification,
    revision:{resultId:'core-'+tests.length+'-'+Math.random(),resultRevision:1,currentRevision:1,...revision},familyAvailability:availability});
  return finalizer.registerFinalizedCoordinateResult(finalizer.finalizeCoordinateResult(input));
}
function complete(result) {
  assert.ok(result.resultId && result.geometryHash && result.geometry);
  assert.ok(Number.isSafeInteger(result.resultRevision) && result.resultRevision>0);
  assert.equal(result.geometryHash,finalizer.createGeometryHash(result.geometry));
}
function allowed(result) {
  complete(result); assert.equal(result.kmlReady,true);
  const out=adapter.adapt(result); assert.equal(out.ok,true,JSON.stringify(out));
  assert.equal(out.geometry.gate.decisionState,result.decisionState);
  assert.deepEqual(out.geometry.geometry,result.geometry);
  assert.equal(new MapPreviewAdapter().adapt(result,{expectedIdentity:result}).previewEligibility.allowed,true);
}
test('handwritten 16 rows produce server lat/lon and canonical identity',()=>{
  assert.equal(points.length,16); assert.ok(points.every(p=>Number.isFinite(p.lat)&&Number.isFinite(p.lon)));
  assert.ok(Math.abs(points[15].lat-(11+27/60+45.09/3600))<1e-12);
  const result=make(); complete(result); assert.equal(result.geometry.coordinates[0].length,17);
});
test('handwritten review warning permits map/KML without AUTO_EXPORT',()=>{const r=make();allowed(r);assert.equal(r.decisionState,'REVIEW_REQUIRED');assert.ok(adapter.adapt(r).geometry.warnings.length);});
test('truthy incomplete frontend identity fails every mandatory field',()=>{
  const result=make(); for(const key of ['resultId','resultRevision','geometryHash','geometry']) assert.equal(browser.getFinalizedCoordinateIdentity({...result,[key]:null}),null);
  assert.match(extract(html,'ensureManualInputFinalized'),/if \(getFinalizedCoordinateIdentity\(\)\)/);
  assert.match(extract(html,'ensureManualInputFinalized'),/recoveryIdentity/);
});
test('canonical displayed coordinates are derived from the server geometry',()=>{const r=make();assert.equal(browser.getCanonicalCoordinateDisplayText(r).split('\n').length,16);assert.match(html,/getCanonicalCoordinateDisplayText\(data.finalizedCoordinateResult\)/);});
const gkRows=runtime.getKyrgyzGkInfo(kyrgyz).rows;
const gkPoints=gkRows.map((r,i)=>boundary.parseStructuredBoundaryPoint(`${r.point} | ${r.x} | ${r.y}`,'kyrgyzstan_gk',i));
test('Kyrgyz historical specialized parser retains order and server projection',()=>{
  assert.equal(gkPoints.length,3);assert.equal(String(gkRows[0].point),'1');
  const p=convertKyrgyzGkToWgs84(13261341,4607777);
  assert.ok(p.longitude>69&&p.longitude<80&&p.latitude>39&&p.latitude<43);
  assert.equal(gkPoints[0].lon,p.longitude);assert.equal(gkPoints[0].source_crs.id,'EPSG:28413');
  allowed(make({structured:engine('kyrgyzstan_gk',gkPoints)}));
});
test('Provider unavailable is not authority rejection for deterministic Kyrgyz',()=>{
  allowed(make({structured:engine('kyrgyzstan_gk',gkPoints),availability:{family:'kyrgyz_gk',status:'BLOCKED_BY_PROVIDER',reasonCode:'FAMILY_BLOCKED_BY_PROVIDER'}}));
  assert.equal(finalizer.getFamilyAvailability('kyrgyz_gk').status,'AVAILABLE');
});
test('Kyrgyz conversion failure does not become geometry',()=>{
  const bad=boundary.parseStructuredBoundaryPoint('1 | 1 | 2','kyrgyzstan_gk');assert.equal(bad.transformStatus,'FAILED');
  const r=make({structured:engine('kyrgyzstan_gk',[bad,...gkPoints.slice(1)])});assert.equal(r.kmlReady,false);assert.equal(r.geometry,null);assert.equal(adapter.adapt(r).ok,false);
});
test('clean printed 16-line DMS never becomes handwriting by count',()=>{
  assert.equal(runtime.getHandwrittenDmsInfo(cleanPrinted,cleanPrinted,{isOcrImage:true}).isHandwrittenDms,false);
  assert.equal(runtime.getHandwrittenDmsVisionRoutingEvidence(cleanPrinted,cleanPrinted).shouldRetry,false);
});
test('printed projected+DMS excludes handwriting and missing CRS stays unresolved',()=>{
  assert.equal(runtime.getHandwrittenDmsInfo(unresolved,unresolved,{isOcrImage:true}).isHandwrittenDms,false);
  const r=routing.getPrintedProjectedDmsReference(unresolved);assert.equal(r.geometrySource,'DMS_DOCUMENT_REFERENCE');
  assert.equal(r.projectedSourceStatus,'UNRESOLVED');assert.equal(r.sourceCrs,null);assert.equal(r.projectedTransformExecuted,false);
  assert.equal(routing.getPrintedProjectedDmsReference(projected),null);
});
test('exact historical handwritten acquisition stays handwritten',()=>assert.equal(runtime.getHandwrittenDmsInfo(handwritten,cleanPrinted,{isOcrImage:true}).isHandwrittenDms,true));
for(const type of ['standard_dms_table','decimal_latlon','kyrgyzstan_gk']) test('family-neutral ordinary warning: '+type,()=>allowed(make({structured:engine(type,[{lat:41,lon:75}])})));
for(const [name,options] of [
  ['invalid geometry',{structured:engine('standard_dms_table',[{lat:91,lon:75}])}],
  ['nonfinite geometry',{structured:engine('standard_dms_table',[{lat:NaN,lon:75}])}],
  ['missing geometry',{structured:{groups:[]}}],
  ['invalid CRS',{recognition:{invalidCrsConfirmation:true}}],
  ['authority rejection',{recognition:{explicitAuthorityRejected:true}}],
  ['technical transform failure',{recognition:{transformStatus:'FAILED'}}],
  ['stale revision',{revision:{resultRevision:1,currentRevision:2}}],
  ['authority confirmation rejected',{revision:{confirmationStatus:'rejected'}}]
]) test('hard blocker: '+name,()=>{const r=make(options);assert.equal(r.kmlReady,false);assert.equal(adapter.adapt(r).ok,false);});
test('adapter rejects missing identity, hash mismatch and forged authority',()=>{
  const r=make(); for(const patch of [{resultId:null},{resultRevision:null},{geometryHash:null},{geometry:null},{geometryHash:'sha256:wrong'},{sourceAuthority:'coordinate_engine_v3'},{decisionState:'AUTO_EXPORT'},{geometry:{type:'Point',coordinates:[1,2]}}]) assert.equal(adapter.adapt({...r,...patch}).ok,false);
});
test('edit complete identity allows warning; stale old revision and incomplete edit do not',()=>{
  const r=make({revision:{resultId:'core-edit'}});allowed(r);
  const edited=make({structured:engine('standard_dms_table',[{lat:41,lon:75}]),revision:{resultId:r.resultId,resultRevision:2,currentRevision:2,confirmationStatus:'pending'}});
  allowed(edited);assert.equal(edited.decisionState,'REVIEW_REQUIRED');assert.notEqual(edited.geometryHash,r.geometryHash);
  assert.equal(adapter.adapt(r).ok,false);assert.equal(adapter.adapt({...edited,geometryHash:null}).ok,false);
});
test('V3 without production authority remains blocked',()=>{
  const r=finalizer.finalizeCoordinateResult(finalizer.createV3FinalizerInput({coordinateEngineV3:engine(),verification:{status:'REVIEW'}}));
  finalizer.registerFinalizedCoordinateResult(r);assert.equal(r.kmlReady,false);assert.equal(adapter.adapt(r).ok,false);
});
test('Madagascar 32 source rows, 32 cells and MultiPolygon remain intact',()=>{
  const record=replay.records.find(r=>r.caseId.includes('madagascar'));
  const parsed=routing.extractMadagascarCadastralRows(record.approvedAcquisitionLines.join('\n'));
  assert.equal(parsed.length,32); const cells=routing.buildMadagascarCadastralCellPolygons(parsed);assert.equal(cells.length,32);
  const structured={coordinate_type:'madagascar_cadastral_grid',requires_review:true,groups:cells.map((c,i)=>({group_id:'m'+i,geometry:'polygon',requires_review:true,kml_ready:false,points:c.points}))};
  const r=make({structured});assert.equal(r.geometry.type,'MultiPolygon');assert.equal(r.geometry.coordinates.length,32);allowed(r);
  assert.equal(routing.hasMadagascarMapGridTickTakeover('290625 295625'),true);
});

async function httpScenario(scenario, run) {
  const child=spawn(process.execPath,[fileURLToPath(import.meta.url),'--http',scenario],{cwd:fileURLToPath(new URL('..',import.meta.url)),windowsHide:true,stdio:['ignore','pipe','pipe','ipc'],
    env:{SystemRoot:process.env.SystemRoot,PATH:process.env.PATH,NODE_ENV:'test',PORT:'0',ENABLE_REGRESSION_TEST_MODE:'true',ALIYUN_API_KEY:'local-mock-only',ALIYUN_BASE_URL:'http://127.0.0.1:1/v1',DOTENV_CONFIG_PATH:'__no_core_test_env__'}});
  child.stdout.resume();child.stderr.resume();const signal=AbortSignal.timeout(25000);
  try{const [{port}]=await once(child,'message',{signal});
    const post=async(route,body,form=false)=>{const response=await fetch(`http://127.0.0.1:${port}${route}`,{method:'POST',headers:form?{'x-regression-test':'1'}:{'content-type':'application/json'},body:form?body:JSON.stringify(body),signal});return {status:response.status,payload:await response.json()};};
    await run(post);
  }finally{const ended=once(child,'exit');child.kill();await ended;}
}
for(const scenario of ['handwritten','kyrgyz','unresolved']) test('HTTP mocked acquisition canonical result: '+scenario,()=>httpScenario(scenario,async post=>{
  const form=new FormData();form.set('visitorId','coordinate-regression-core-p0');form.set('image',new Blob([new Uint8Array([255,216,255,217])],{type:'image/jpeg'}),'synthetic.jpg');
  if(scenario==='kyrgyz')form.set('rawHint','Kyrgyzstan Gauss Kruger № points X Y');
  const {status,payload}=await post('/api/recognize-coordinates',form,true);assert.equal(status,200,JSON.stringify(payload));
  const result=payload.finalizedCoordinateResult;complete(result);assert.equal(result.kmlReady,true,JSON.stringify({type:payload.coordinateEngineV2?.coordinate_type,reasons:result.reasonCodes}));
  if(scenario==='handwritten'){assert.equal(payload.coordinateEngineV2.coordinate_type,'handwritten_dms_experimental');assert.equal(result.requiresReview,true,JSON.stringify({decision:result.decisionState,quality:result.qualityGateStatus,availability:result.availabilityStatus,policy:result.familySafetyPolicy,engineReview:payload.coordinateEngineV2.requires_review}));}
  if(scenario==='kyrgyz')assert.equal(payload.coordinateEngineV2.coordinate_type,'kyrgyzstan_gk');
  if(scenario==='unresolved'){assert.equal(payload.geometrySource,'DMS_DOCUMENT_REFERENCE');assert.equal(payload.projectedSourceStatus,'UNRESOLVED');}
}));
test('HTTP manual edit, recovery and stale/hash guards',()=>httpScenario('manual',async post=>{
  const start=await post('/api/coordinate-manual-finalize',{coordinateText:'75,41\n75.01,41\n75.01,41.01\n75,41.01',requireConfirmation:true});assert.equal(start.status,200);const r=start.payload.finalizedCoordinateResult;complete(r);assert.equal(r.kmlReady,true);
  const recovered=await post('/api/coordinate-manual-finalize',{coordinateText:cleanPrinted,recoveryIdentity:{resultId:r.resultId,resultRevision:r.resultRevision}});assert.equal(recovered.payload.finalizedCoordinateResult.geometryHash,r.geometryHash);
  const edited=await post('/api/coordinate-revision',{resultId:r.resultId,resultRevision:r.resultRevision,geometryHash:r.geometryHash,coordinateText:'75,41',requireConfirmation:true});assert.equal(edited.status,200);assert.equal(edited.payload.finalizedCoordinateResult.kmlReady,true);
  for(const identity of [{resultId:r.resultId,resultRevision:1,geometryHash:r.geometryHash},{resultId:r.resultId,resultRevision:2,geometryHash:'wrong'}]) {
    const bad=await post('/api/coordinate-manual-finalize',{coordinateText:cleanPrinted,recoveryIdentity:identity});assert.equal(bad.status,409);
  }
}));
test('HTTP incomplete DMS recovery cannot erase technical or authority blockers',()=>httpScenario('manual',async post=>{
  for(const name of ['missing','authority','crs','transform','rejected','v3']) {
    const response=await post('/api/coordinate-manual-finalize',{coordinateText:'75,41\n75.01,41\n75.01,41.01\n75,41.01',requireConfirmation:true,
      recoveryIdentity:{resultId:'core-recovery-'+name,resultRevision:1}});
    assert.equal(response.status,name==='missing'?200:422,name);
    if(name==='missing'){complete(response.payload.finalizedCoordinateResult);assert.equal(response.payload.finalizedCoordinateResult.resultRevision,2);assert.equal(response.payload.finalizedCoordinateResult.kmlReady,true);}
  }
}));
for(const {name,fn} of tests){await fn();console.log('PASS '+name);}
console.log(`Production Core Closure P0: ${tests.length}/${tests.length} PASS; REAL_PROVIDER_CALLS=0`);

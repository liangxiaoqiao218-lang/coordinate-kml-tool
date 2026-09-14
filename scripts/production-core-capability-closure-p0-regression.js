import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {fileURLToPath} from 'node:url';
import {inflateSync} from 'node:zlib';
import vm from 'node:vm';
import * as routing from '../server/recognition/family-primary-routing.js';
import * as dmsSourceStructure from '../server/recognition/dms-source-structure.js';
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
const syntheticPng = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=', 'base64');

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
    if (scenario === 'ocr-failure') throw new Error('MOCK_PROVIDER_UNAVAILABLE');
    const content = scenario === 'kyrgyz' ? kyrgyz : scenario === 'unresolved' ? unresolved : handwritten;
    return new Response(JSON.stringify({choices:[{message:{content}}]}), {status:200,headers:{'content-type':'application/json'}});
  };
  const listen = http.Server.prototype.listen;
  http.Server.prototype.listen = function(port, cb) {
    this.once('listening', () => process.send({port:this.address().port}));
    return listen.call(this,port,'127.0.0.1',cb);
  };
  process.on('message',message=>{if(message==='stats')process.send({calls,ocrCalls:Number(globalThis.__coreOcrCalls||0)});});
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
const runtime = vm.createContext({...routing,...dmsSourceStructure,...boundary,Buffer,inflateSync,pngCrcTable:null});
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
test('Provider handwritten wording alone cannot authorize handwritten acquisition',()=>assert.equal(runtime.getHandwrittenDmsInfo(handwritten,cleanPrinted,{isOcrImage:true}).isHandwrittenDms,false));
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
  const ocrProbePreload=['ocr-failure','post-provider-failure'].includes(scenario)?`import {registerHooks} from 'node:module';
registerHooks({load(url,context,nextLoad){
  const result=nextLoad(url,context);
  if(!url.replace(/\\\\/g,'/').endsWith('/node_modules/tesseract.js/src/index.js'))return result;
  return {...result,source:"module.exports={createWorker:async()=>{globalThis.__coreOcrCalls=(globalThis.__coreOcrCalls||0)+1;return {recognize:async()=>{throw new Error('PRIVATE_DECODER_DETAIL')},terminate:async()=>{}}}};"};
}});`:null;
  const child=spawn(process.execPath,[...(ocrProbePreload?['--import',`data:text/javascript,${encodeURIComponent(ocrProbePreload)}`]:[]),fileURLToPath(import.meta.url),'--http',scenario],{cwd:fileURLToPath(new URL('..',import.meta.url)),windowsHide:true,stdio:['ignore','pipe','pipe','ipc'],
    env:{SystemRoot:process.env.SystemRoot,PATH:process.env.PATH,NODE_ENV:'test',PORT:'0',ENABLE_REGRESSION_TEST_MODE:'true',ALIYUN_API_KEY:'local-mock-only',ALIYUN_BASE_URL:'http://127.0.0.1:1/v1',DOTENV_CONFIG_PATH:'__no_core_test_env__'}});
  child.stdout.resume();child.stderr.resume();const signal=AbortSignal.timeout(25000);
  try{const [{port}]=await once(child,'message',{signal});
    const post=async(route,body,form=false)=>{const response=await fetch(`http://127.0.0.1:${port}${route}`,{method:'POST',headers:form?{'x-regression-test':'1',...(scenario==='post-provider-failure'?{'x-coordinate-regression-failure':'POST_PROVIDER_INTERNAL_FAILURE'}:{})}:{'content-type':'application/json'},body:form?body:JSON.stringify(body),signal});return {status:response.status,payload:await response.json()};};
    post.get=async route=>{const response=await fetch(`http://127.0.0.1:${port}${route}`,{signal});return {status:response.status,payload:await response.json()};};
    post.stats=async()=>{const pending=once(child,'message',{signal});child.send('stats');const [value]=await pending;return value;};
    await run(post);
  }finally{const ended=once(child,'exit');child.kill();await ended;}
}
for(const scenario of ['handwritten','kyrgyz','unresolved']) test('HTTP mocked acquisition preserves authority boundaries: '+scenario,()=>httpScenario(scenario,async post=>{
  const form=new FormData();form.set('visitorId','coordinate-regression-core-p0');form.set('image',new Blob([syntheticPng],{type:'image/png'}),'synthetic.png');
  if(scenario==='kyrgyz')form.set('rawHint','Kyrgyzstan Gauss Kruger № points X Y');
  const {status,payload}=await post('/api/recognize-coordinates',form,true);assert.equal(status,200,JSON.stringify(payload));
  const result=payload.finalizedCoordinateResult;
  if(scenario==='handwritten'){
    assert.equal(payload.coordinateEngineV2.coordinate_type === 'handwritten_dms_experimental',false);
    assert.equal(payload.coordinates,'');
    assert.deepEqual(payload.coordinateEngineV2.groups,[]);
    assert.ok(result);
    assert.equal(result.geometry,null);
    assert.equal(result.geometryHash,null);
    assert.equal(result.decisionState,'BLOCKED');
    assert.equal(result.kmlReady,false);
    assert.ok(result.blockingReasons.some(reason=>reason.code==='STRUCTURED_GEOMETRY_MISSING'));
    assert.ok(result.blockingReasons.some(reason=>reason.code==='QUALITY_GATE_FAILED'));
    assert.ok(result.blockingReasons.some(reason=>reason.code==='KML_NOT_READY'));
    assert.equal(adapter.adapt(result).ok,false);
    assert.equal(new MapPreviewAdapter().adapt(result,{expectedIdentity:result}).previewEligibility.allowed,false);
    return;
  }
  if(scenario==='unresolved'){
    const expectedSourceRows=[
      {label:'1',latitudeDms:'2°31\'21.134" S',longitudeDms:'119°30\'40.863" E'},
      {label:'2',latitudeDms:'2°31\'21.116" S',longitudeDms:'119°30\'50.018" E'},
      {label:'3',latitudeDms:'2°31\'26.910" S',longitudeDms:'119°30\'50.029" E'},
      {label:'4',latitudeDms:'2°31\'26.928" S',longitudeDms:'119°30\'40.874" E'}
    ];
    assert.equal(payload.geometrySource,'DMS_DOCUMENT_REFERENCE');
    assert.equal(payload.projectedSourceStatus,'UNRESOLVED');
    assert.equal(payload.explicitAuthorityRejected,true);
    assert.equal(payload.requiresReview,true);
    assert.equal(payload.coordinates,'');
    assert.equal(payload.documentReference.sourceRows.length,4);
    assert.deepEqual(payload.documentReference.sourceRows.map(({label,latitudeDms,longitudeDms})=>({label,latitudeDms,longitudeDms})),expectedSourceRows);
    assert.equal(payload.imageDmsSourceCompleteness.failClosed,true);
    assert.ok(result);
    assert.equal(result.geometry,null);
    assert.equal(result.geometryHash,null);
    assert.equal(result.kmlReady,false);
    assert.equal(result.decisionState,'BLOCKED');
    assert.equal(adapter.adapt(result).ok,false);
    assert.equal(new MapPreviewAdapter().adapt(result,{expectedIdentity:result}).previewEligibility.allowed,false);
    return;
  }
  complete(result);assert.equal(result.kmlReady,true,JSON.stringify({type:payload.coordinateEngineV2?.coordinate_type,reasons:result.reasonCodes}));
  if(scenario==='kyrgyz')assert.equal(payload.coordinateEngineV2.coordinate_type,'kyrgyzstan_gk');
}));
test('HTTP malformed image fails closed before Provider and service remains alive',()=>httpScenario('malformed',async post=>{
  const truncatedPng=Buffer.alloc(24);Buffer.from([0x89,0x50,0x4e,0x47,0x0d,0x0a,0x1a,0x0a]).copy(truncatedPng);truncatedPng.write('IHDR',12,'ascii');truncatedPng.writeUInt32BE(1,16);truncatedPng.writeUInt32BE(1,20);
  const truncatedJpeg=Buffer.from([0xff,0xd8,0xff,0xc0,0x00,0x08,0x08,0x00,0x01,0x00,0x01,0x01]);
  const crcDamagedPng=Buffer.from(syntheticPng);crcDamagedPng[crcDamagedPng.length-1]^=1;
  const fakeJpeg=Buffer.from([0xff,0xd8,0xff,0xdb,0x00,0x03,0x00,0xff,0xc4,0x00,0x03,0x00,0xff,0xc0,0x00,0x08,0x08,0x00,0x01,0x00,0x01,0x01,0xff,0xda,0x00,0x02,0x01,0x02,0x03,0x04,0xff,0xd9]);
  const fakeGif=Buffer.from([0x47,0x49,0x46,0x38,0x39,0x61,1,0,1,0,0,0,0,0x2c,0,0,0,0,1,0,1,0,0,2,1,0,0,0x3b]);
  const fakeBmp=Buffer.alloc(54);fakeBmp.write('BM',0,'ascii');fakeBmp.writeUInt32LE(54,2);fakeBmp.writeUInt32LE(54,10);fakeBmp.writeUInt32LE(40,14);fakeBmp.writeInt32LE(1,18);fakeBmp.writeInt32LE(1,22);fakeBmp.writeUInt16LE(1,26);fakeBmp.writeUInt16LE(24,28);
  const fakeWebp=Buffer.alloc(30);fakeWebp.write('RIFF',0,'ascii');fakeWebp.writeUInt32LE(22,4);fakeWebp.write('WEBPVP8 ',8,'ascii');fakeWebp.writeUInt32LE(10,16);fakeWebp.set([0,0,0,0x9d,0x01,0x2a,1,0,1,0],20);
  const fakeHeif=Buffer.alloc(38);fakeHeif.writeUInt32BE(16,0);fakeHeif.write('ftypmif1',4,'ascii');fakeHeif.writeUInt32BE(13,16);fakeHeif.write('meta',20,'ascii');fakeHeif.writeUInt32BE(9,29);fakeHeif.write('mdat',33,'ascii');
  const invalidImages=[
    [Buffer.from('not-an-image'),'image/png','malformed.png'],
    [truncatedPng,'image/png','truncated.png'],
    [truncatedJpeg,'image/jpeg','truncated.jpg'],
    [crcDamagedPng,'image/png','crc-damaged.png'],
    [fakeJpeg,'image/jpeg','container-only.jpg'],
    [fakeGif,'image/gif','container-only.gif'],
    [fakeBmp,'image/bmp','container-only.bmp'],
    [fakeWebp,'image/webp','container-only.webp'],
    [fakeHeif,'image/heif','container-only.heif']
  ];
  for(const [bytes,type,name] of invalidImages){
    const form=new FormData();form.set('visitorId','coordinate-regression-core-p0');form.set('image',new Blob([bytes],{type}),name);
    const result=await post('/api/recognize-coordinates',form,true);
    assert.equal(result.status,400);assert.equal(result.payload.success,false);assert.equal(result.payload.code,'COORDINATE_IMAGE_INVALID');
    assert.equal(result.payload.rawText,'');assert.equal(result.payload.coordinates,'');
  }
  assert.equal((await post.stats()).calls,0);
  const version=await post.get('/api/version');assert.equal(version.status,200);assert.ok(version.payload.runtimeIdentity);
}));
test('coordinate image preflight accepts only strictly validated container types',()=>{
  assert.match(source,/mimeType === "image\/png"[\s\S]*?hasValidPngStructure/);
  assert.match(source,/\["image\/jpeg", "image\/jpg"\][\s\S]*?hasValidJpegStructure/);
  assert.match(source,/\["image\/bmp", "image\/x-ms-bmp"\][\s\S]*?hasValidBmpStructure/);
  assert.doesNotMatch(source,/mimeType === "image\/gif"[\s\S]{0,120}hasValidGifStructure/);
  assert.doesNotMatch(source,/mimeType === "image\/webp"[\s\S]{0,120}hasValidWebpStructure/);
  assert.doesNotMatch(source,/\["image\/heic", "image\/heif"\][\s\S]{0,120}hasValidHeifStructure/);
});
test('frozen non-customer JPEG and PNG fixtures pass the actual strict preflight',async()=>{
  const fixtures=[
    ['../regression-samples/OCR_GOLDEN/fixtures/indonesia-utm50s-real-001.jpg','image/jpeg','2f508653305fee7c08470218f9bf94f75b56d26d7b28edcd7d8d68cd8f88eaf6'],
    ['../regression-samples/production-recognition-recovery-p0/indonesia-utm50s-real-002.jpg','image/jpeg','707e971aef6e5a6744cbd860cf701e41218fe6fb9a609b88e8bd121d03348b5a'],
    ['../regression-samples/fixtures/马达加斯加坐标.png','image/png','ef023b37d07676437cc24804c70a8681d851974828f1bb232538aa222e36ec5e']
  ];
  for(const [relative,mimetype,sha256] of fixtures){
    const buffer=await readFile(new URL(relative,import.meta.url));
    assert.equal(createHash('sha256').update(buffer).digest('hex'),sha256);
    const validation=runtime.validateCoordinateImageUpload({buffer,mimetype});
    assert.equal(validation.valid,true);assert.equal(validation.reason,'VALID_IMAGE_STRUCTURE');
  }
});
test('HTTP local OCR failure is sanitized fail-closed and service remains alive',()=>httpScenario('ocr-failure',async post=>{
  const form=new FormData();form.set('visitorId','coordinate-regression-core-p0');form.set('image',new Blob([syntheticPng],{type:'image/png'}),'synthetic.png');
  const result=await post('/api/recognize-coordinates',form,true);
  assert.equal(result.status,422);assert.equal(result.payload.success,false);assert.equal(result.payload.code,'LOCAL_OCR_FAILED');
  assert.equal(result.payload.reason,'local_ocr_failed');assert.equal(result.payload.rawText,'');assert.equal(result.payload.coordinates,'');
  assert.equal(JSON.stringify(result.payload).includes('PRIVATE_DECODER_DETAIL'),false);const stats=await post.stats();assert.equal(stats.calls,1);assert.equal(stats.ocrCalls,1);
  const version=await post.get('/api/version');assert.equal(version.status,200);assert.ok(version.payload.runtimeIdentity);
}));
test('HTTP post-Provider internal failure cannot transfer control to local OCR',()=>httpScenario('post-provider-failure',async post=>{
  const form=new FormData();form.set('visitorId','coordinate-regression-core-p0');form.set('image',new Blob([syntheticPng],{type:'image/png'}),'synthetic.png');
  const result=await post('/api/recognize-coordinates',form,true);
  assert.equal(result.status,422);assert.equal(result.payload.success,false);assert.equal(result.payload.code,'COORDINATE_POST_PROVIDER_PROCESSING_FAILED');
  assert.equal(result.payload.reason,'post_provider_processing_failed');assert.equal(result.payload.rawText,'');assert.equal(result.payload.coordinates,'');
  assert.equal(JSON.stringify(result.payload).includes('REGRESSION_POST_PROVIDER_INTERNAL_FAILURE'),false);
  const stats=await post.stats();assert.equal(stats.calls,1);assert.equal(stats.ocrCalls,0);
  const version=await post.get('/api/version');assert.equal(version.status,200);assert.ok(version.payload.runtimeIdentity);
}));
test('production runtime imports retry classification and sanitizes post-Provider failures',()=>{
  assert.match(source,/DMS_RETRY_ROUTE_CLASSIFICATION[\s\S]*?from "\.\/server\/recognition\/dms-source-structure\.js"/);
  assert.match(source,/if \(stage1ProviderSucceeded\)[\s\S]*?COORDINATE_POST_PROVIDER_PROCESSING_FAILED/);
  assert.doesNotMatch(source,/debugErrorMessage/);
});
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

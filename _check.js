
let TK=null,US=null,PID=null,DT={},VW='overview',CO=false,CH=[],CTS={},_liveTimer=null;
const RCLR={'Stable Growth':'#1E6B2E','Commodity Expansion':'#2E75B6','USD Tightening':'#C9A227','Sovereign Stress':'#E67E22','Systemic Crisis':'#C0392B'};

async function api(path,opts={}){const hd={'Content-Type':'application/json'};if(TK)hd['Authorization']='Bearer '+TK;const r=await fetch(path,{...opts,headers:hd});if(!r.ok)throw new Error('API '+r.status);return r.json();}
function $(s){return document.querySelector(s)}
// Safe number formatter — returns '—' for null/NaN, otherwise fixed decimals
function fmt(v,d=2){if(v==null||v===''||isNaN(+v))return '—';return (+v).toFixed(d);}
function fmtPct(v,d=2){const s=fmt(v,d);return s==='—'?s:s+'%';}
function el(tag,attrs){const e=document.createElement(tag);const args=Array.from(arguments).slice(2);if(attrs){if(attrs.cl)e.className=attrs.cl;if(attrs.st)Object.assign(e.style,attrs.st);if(attrs.id)e.id=attrs.id;Object.keys(attrs).forEach(k=>{if(k.startsWith('on'))e.addEventListener(k.slice(2).toLowerCase(),attrs[k]);else if(!['cl','st','id'].includes(k))e.setAttribute(k,attrs[k])});}
// Fix: any non-null child that is not a Node must become a text node (handles numbers, booleans, etc.)
args.forEach(c=>{if(c==null||c===false)return;if(Array.isArray(c))c.forEach(x=>{if(x==null||x===false)return;if(x instanceof Node)e.appendChild(x);else e.appendChild(document.createTextNode(String(x)));});else if(c instanceof Node)e.appendChild(c);else e.appendChild(document.createTextNode(String(c)));});return e;}
function kpi(t,v,u,c,s){return el('div',{cl:'cd fi'},el('div',{cl:'cd-t',st:{marginBottom:'6px'}},t),el('div',{st:{display:'flex',alignItems:'baseline',gap:'2px'}},el('span',{cl:'cv',st:{color:c||'var(--ac)'}},String(v??'—')),el('span',{st:{fontSize:'10px',color:'var(--t3)'}},u||'')),el('div',{cl:'cl2'},s||''));}
function bar(v,mx,c){const o=el('div',{cl:'bh'});o.appendChild(el('div',{cl:'bf',st:{width:Math.min(v/(mx||100)*100,100)+'%',background:c||'var(--ac)'}}));return o;}
function destroyCharts(){Object.values(CTS).forEach(c=>{try{c.destroy()}catch(e){}});CTS={};}
function mkLine(id,labels,ds){const cv=document.getElementById(id);if(!cv)return;CTS[id]=new Chart(cv,{type:'line',data:{labels,datasets:ds.map(d=>({label:d.l,data:d.d,borderColor:d.c,backgroundColor:d.fill?d.c.replace(')',',0.08)').replace('rgb','rgba'):'transparent',borderWidth:1.5,pointRadius:0,tension:.3,fill:!!d.fill}))},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:ds.length>1,labels:{color:'#586880',font:{size:8},boxWidth:8}},tooltip:{mode:'index',intersect:false,backgroundColor:'#1e2a3c',titleColor:'#e8ecf4',bodyColor:'#8d9db8',titleFont:{size:9},bodyFont:{size:9}}},scales:{x:{grid:{color:'rgba(37,48,69,.2)'},ticks:{color:'#586880',font:{size:7},maxTicksLimit:8},border:{display:false}},y:{grid:{color:'rgba(37,48,69,.2)'},ticks:{color:'#586880',font:{size:7}},border:{display:false}}},interaction:{intersect:false,mode:'index'}}});}
function mkBar(id,labels,data,colors){const cv=document.getElementById(id);if(!cv)return;CTS[id]=new Chart(cv,{type:'bar',data:{labels,datasets:[{data,backgroundColor:colors,borderWidth:0,borderRadius:2}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{grid:{display:false},ticks:{color:'#586880',font:{size:7}},border:{display:false}},y:{grid:{color:'rgba(37,48,69,.2)'},ticks:{color:'#586880',font:{size:7}},border:{display:false}}}}});}
function mkDoughnut(id,labels,data,colors){const cv=document.getElementById(id);if(!cv)return;CTS[id]=new Chart(cv,{type:'doughnut',data:{labels,datasets:[{data,backgroundColor:colors,borderWidth:0}]},options:{responsive:true,maintainAspectRatio:false,cutout:'65%',plugins:{legend:{position:'right',labels:{color:'#8d9db8',font:{size:8},boxWidth:7,padding:5}}}}});}
function mkStacked(id,labels,ds){const cv=document.getElementById(id);if(!cv)return;CTS[id]=new Chart(cv,{type:'line',data:{labels,datasets:ds.map(d=>({label:d.l,data:d.d,borderColor:d.c,backgroundColor:d.c,borderWidth:1,pointRadius:0,fill:true,tension:.3}))},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:'#586880',font:{size:7},boxWidth:7}}},scales:{x:{grid:{display:false},ticks:{color:'#586880',font:{size:7},maxTicksLimit:8},border:{display:false}},y:{stacked:true,grid:{color:'rgba(37,48,69,.15)'},ticks:{color:'#586880',font:{size:7}},max:1,border:{display:false}}}}});}

// LOGIN
function showLogin(){$('#root').innerHTML='';$('#root').appendChild(el('div',{cl:'lw'},el('div',{cl:'lb'},el('div',{cl:'lb-logo'},'AfriSK'),el('div',{cl:'lb-sub'},'African Structural Risk Intelligence — InteliRisk v4 Engine'),el('div',{cl:'fg'},el('label',{cl:'fl'},'Username'),el('input',{cl:'fin',id:'lu',type:'text',value:'admin'})),el('div',{cl:'fg'},el('label',{cl:'fl'},'Password'),el('input',{cl:'fin',id:'lp',type:'password',value:'admin123'})),el('div',{id:'le',cl:'lerr'}),el('button',{cl:'btn',id:'lbtn',onClick:doLogin},'Sign In'),el('div',{cl:'lhint'},'admin/admin123 · institution/inst123 · retail/retail123'))));document.querySelectorAll('.fin').forEach(x=>x.addEventListener('keydown',e=>{if(e.key==='Enter')doLogin()}));}
async function doLogin(){const b=$('#lbtn');b.disabled=true;b.textContent='Authenticating...';try{const d=await api('/api/v1/auth/login',{method:'POST',body:JSON.stringify({username:$('#lu').value,password:$('#lp').value})});TK=d.access_token;US={username:d.username,role:d.role};await initApp();}catch(e){$('#le').textContent='Invalid credentials';b.disabled=false;b.textContent='Sign In';}}
async function initApp(){buildLayout();try{const pr=await api('/api/v1/portfolios');if(!pr.portfolios.length){const c=await api('/api/v1/portfolios/create-sample',{method:'POST'});PID=c.portfolio_id;}else PID=pr.portfolios[0].portfolio_id;showLoading();DT=await api('/api/v1/portfolios/'+PID+'/compute-all');console.log('Data loaded:',Object.keys(DT));buildLayout();renderView();}catch(e){console.error('Init error:',e);$('#ct').innerHTML='<div class="ld">Error: '+e.message+'</div>';}}
function showLoading(){const c=$('#ct');if(c)c.innerHTML='<div class="ld"><div class="sp"></div><div style="font-size:11px">Computing 8 layers (InteliRisk v4)...</div></div>';}
async function recompute(){showLoading();try{DT=await api('/api/v1/portfolios/'+PID+'/compute-all');buildLayout();renderView();}catch(e){console.error(e);}}

const TITLES={overview:'Dashboard Overview',risk:'Portfolio Risk Core',structural:'Structural & Network',factors:'African Factor Model',regime:'HMM Regime Detection',stress:'Stress Simulator',intel:'Intelligence Report',analytics:'Portfolio Analytics',holdings:'Portfolio Holdings',upload:'Upload Portfolio',trades:'Bloomberg & Live Data'};
const NAVS=[{id:'overview',i:'◈',l:'Overview',s:'DASHBOARD'},{id:'analytics',i:'▣',l:'Analytics',s:'DASHBOARD'},{id:'risk',i:'△',l:'Risk Core',s:'ANALYTICS'},{id:'structural',i:'◎',l:'Structural',s:'ANALYTICS'},{id:'factors',i:'⊞',l:'Factors',s:'ANALYTICS'},{id:'regime',i:'◆',l:'Regime',s:'ANALYTICS'},{id:'stress',i:'⚡',l:'Stress Test',s:'ANALYTICS'},{id:'intel',i:'★',l:'Intelligence',s:'INSIGHTS'},{id:'holdings',i:'☰',l:'Holdings',s:'PORTFOLIO'},{id:'upload',i:'⊕',l:'Upload',s:'PORTFOLIO'},{id:'trades',i:'⇄',l:'Bloomberg',s:'DATA'}];

function buildLayout(){const r=$('#root');r.innerHTML='';let cs='';const ne=[];NAVS.forEach(n=>{if(n.s!==cs){ne.push(el('div',{cl:'ns'},n.s));cs=n.s;}ne.push(el('div',{cl:'ni'+(n.id===VW?' on':''),onClick:()=>{VW=n.id;buildLayout();renderView();}},el('span',{cl:'ni-i'},n.i),el('span',null,n.l),n.id==='intel'&&DT.intelligence?el('span',{cl:'ni-b'},''+(DT.intelligence.alerts||[]).length):null));});
r.appendChild(el('div',{cl:'app'},el('div',{cl:'sb'},el('div',{cl:'sb-hd'},el('div',{cl:'sb-logo'},'AfriSK'),el('div',{cl:'sb-tag'},'InteliRisk v4 Engine')),el('div',{cl:'sb-nav'},ne),el('div',{cl:'sb-ft'},el('div',{cl:'sb-u'},el('div',{cl:'sb-av'},(US||{}).username?US.username[0].toUpperCase():'?'),el('div',null,el('div',{cl:'sb-un'},(US||{}).username||''),el('div',{cl:'sb-ur'},((US||{}).role||'').replace(/_/g,' ')))))),el('div',{cl:'ma'},el('div',{cl:'tb'},el('div',{cl:'tb-t',id:'pt'},TITLES[VW]||''),el('div',{cl:'tb-r'},el('span',{cl:'tb-dot'}),el('span',{cl:'tb-st'},'Bloomberg'),el('button',{cl:'tbtn',onClick:recompute},'⟳ Refresh'),el('button',{cl:'tbtn tbtn-a',onClick:toggleChat},'◉ AI Assistant'))),el('div',{cl:'ct',id:'ct'})),el('div',{cl:'cp'+(CO?' open':''),id:'cpan'},el('div',{cl:'cp-h'},el('span',{cl:'cp-tt'},'AI Assistant'),el('button',{cl:'cp-x',onClick:toggleChat},'✕')),el('div',{cl:'cp-m',id:'cms'}),el('div',{cl:'cp-i'},el('textarea',{cl:'ci',id:'cin',placeholder:'Ask anything...',rows:'1',onKeydown:e=>{if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();sendChat();}}},),el('button',{cl:'cs',onClick:sendChat},'→'))),el('button',{cl:'cfab'+(CO?' hid':''),onClick:toggleChat},'◉')));refreshChatUI();}
function toggleChat(){CO=!CO;const p=$('#cpan');if(p)p.classList.toggle('open',CO);const f=$('.cfab');if(f)f.classList.toggle('hid',CO);}
async function sendChat(){const inp=$('#cin');if(!inp)return;const msg=inp.value.trim();if(!msg)return;CH.push({r:'user',t:msg});inp.value='';refreshChatUI();try{const res=await api('/api/v1/chat',{method:'POST',body:JSON.stringify({message:msg,portfolio_id:PID})});CH.push({r:'assistant',t:res.response});}catch(e){CH.push({r:'assistant',t:'Error: '+e.message});}refreshChatUI();}
function refreshChatUI(){const c=$('#cms');if(!c)return;c.innerHTML='';CH.forEach(m=>{const d=document.createElement('div');d.className='cm '+(m.r==='user'?'cm-u':'cm-a');d.textContent=m.t;c.appendChild(d);});c.scrollTop=c.scrollHeight;}

function renderView(){const ct=$('#ct');if(!ct)return;ct.innerHTML='';destroyCharts();if(_liveTimer&&VW!=='trades'){clearInterval(_liveTimer);_liveTimer=null;}try{({overview:vOverview,analytics:vAnalytics,risk:vRisk,structural:vStructural,factors:vFactors,regime:vRegime,stress:vStress,intel:vIntel,holdings:vHoldings,upload:vUpload,trades:vTrades}[VW]||vOverview)(ct);}catch(e){console.error('View ['+VW+']:',e);ct.innerHTML='<div class="ld" style="color:var(--rd)">Error: '+e.message+'</div>';}}

// === OVERVIEW ===
function vOverview(ct){
const s=DT.score||{},p=(DT.risk_core||{}).portfolio||{},rg=DT.regime||{},it=DT.intelligence||{};
ct.appendChild(el('div',{cl:'g5 mb'},kpi('Risk Score',s.score||0,'/100',s.color,s.level),kpi('Mean Vol',(p.mean_vol_21d||0),'%','var(--bl)','21d Ann.'),kpi('Mean VaR',(p.mean_var||0),'%','var(--rd)','5% Daily'),kpi('Worst DD',(p.worst_dd||0),'%','var(--or)','Peak-Trough'),kpi('Regime',rg.active_regime||'—','',RCLR[rg.active_regime],'HMM Viterbi')));
// Score + Regime + Intel
ct.appendChild(el('div',{cl:'g3 mb'},
el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Score Components')),el('div',null,...Object.entries(s.contributions||{}).map(([k,v])=>el('div',{st:{marginBottom:'7px'}},el('div',{cl:'fb',st:{fontSize:'9px',marginBottom:'2px'}},el('span',{st:{color:'var(--t3)'}},k.replace(/_/g,' ')),el('span',{cl:'mn',st:{color:'var(--t2)'}},((v.contribution||0)>0?'+':'')+v.contribution?.toFixed(2))),bar(Math.abs(v.z_score||0)*33,100,Math.abs(v.z_score||0)>1.5?'var(--rd)':Math.abs(v.z_score||0)>0.5?'var(--or)':'var(--gn)'))))),
el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Regime Probabilities')),el('div',null,...Object.entries(rg.regime_probs||{}).sort((a,b)=>b[1]-a[1]).map(([n,prob])=>el('div',{st:{marginBottom:'8px'}},el('div',{cl:'fb',st:{fontSize:'9px',marginBottom:'2px'}},el('span',null,n),el('span',{cl:'mn',st:{color:RCLR[n]||'var(--t2)'}},(prob*100).toFixed(1)+'%')),bar(prob*100,100,RCLR[n]||'var(--ac)'))))),
el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Intelligence'),el('span',{cl:'cd-b',st:{background:s.color||'var(--ac)',color:'var(--b0)'}},s.level||'')),el('div',{st:{fontSize:'10.5px',lineHeight:'1.6',color:'var(--t2)',marginBottom:'10px'}},it.headline||''),el('div',null,...(it.alerts||[]).slice(0,3).map(a=>el('div',{cl:'al al-'+(a.severity==='HIGH'?'h':'m')},el('div',{cl:'al-sv',st:{color:a.severity==='HIGH'?'var(--rd)':'var(--or)'}},a.severity),el('div',{st:{color:'var(--t2)',fontSize:'10px'}},a.msg)))))));
// Charts
ct.appendChild(el('div',{cl:'g2 mb'},el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Cumulative Return')),el('div',{cl:'cc cc-t'},el('canvas',{id:'ov1'}))),el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Drawdown')),el('div',{cl:'cc cc-t'},el('canvas',{id:'ov2'})))));
// Country scores
const cs2=s.country_scores||{};
if(Object.keys(cs2).length){ct.appendChild(el('div',{cl:'g4'},
...Object.entries(cs2).map(([m,d])=>kpi(d.name||m,(d.score||0).toFixed(1),'/100',d.level==='HIGH'||d.level==='CRITICAL'?'var(--rd)':d.level==='MODERATE'?'var(--or)':'var(--gn)',d.level))));}
setTimeout(()=>{const ts=(DT.risk_core||{}).time_series||{};if(ts.dates&&ts.dates.length){const lb=ts.dates.map(d=>d.slice(5));mkLine('ov1',lb,[{l:'Cumulative',d:ts.cumulative,c:'#d4973a'}]);mkLine('ov2',lb,[{l:'Drawdown',d:ts.drawdown,c:'#f06060',fill:true}]);}},60);}

// === PORTFOLIO ANALYTICS (Portfolio Visualizer–style) ===
let _analyticsCache=null;
async function vAnalytics(ct){
ct.appendChild(el('div',{cl:'ld',id:'analytics-loader'},el('div',{cl:'sp'}),el('div',{st:{fontSize:'11px'}},'Loading portfolio analytics...')));
try{
const data=_analyticsCache||(await api('/api/v1/portfolios/'+PID+'/analytics'));
_analyticsCache=data;
ct.innerHTML='';
const sm=data.summary||{};
const syms=data.symbols||[];
const SCOLORS=['#1F4E79','#2E75B6','#5896f0','#C9A227','#E67E22','#2dd4a0','#9878e8','#f06060','#20c8e0','#d4973a','#f09838','#8E44AD'];

// ── KPI Row ──
ct.appendChild(el('div',{cl:'g5 mb'},
kpi('Total Return',sm.total_return,'%',sm.total_return>=0?'var(--gn)':'var(--rd)',sm.start_date+' → '+sm.end_date),
kpi('Ann. Return',sm.ann_return,'%',sm.ann_return>=0?'var(--gn)':'var(--rd)','CAGR'),
kpi('Ann. Volatility',sm.ann_volatility,'%','var(--bl)','Std Dev × √252'),
kpi('Sharpe Ratio',sm.sharpe,'','var(--pu)','Rf = 0%'),
kpi('Max Drawdown',sm.max_drawdown,'%','var(--rd)','Peak-to-Trough')
));

// ── Portfolio Growth + Asset Allocation ──
ct.appendChild(el('div',{cl:'g2 mb',st:{gridTemplateColumns:'2fr 1fr'}},
el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Portfolio Growth (Cumulative)')),el('div',{cl:'cc',st:{height:'280px'}},el('canvas',{id:'ag1'}))),
el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Asset Allocation')),el('div',{st:{height:'280px'}},el('canvas',{id:'ag2'})))
));

// ── Annual Returns ──
const at=data.annual_table||[];
if(at.length){
const years=at.map(r=>r.year);
ct.appendChild(el('div',{cl:'cd mb fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Annual Returns (%)')),
el('div',{st:{height:'240px'}},el('canvas',{id:'ag3'})),
el('table',{cl:'dt',st:{marginTop:'10px'}},
el('thead',null,el('tr',null,el('th',null,'Year'),...syms.map(s=>el('th',null,s)),el('th',{st:{color:'var(--ac)'}},'Portfolio'))),
el('tbody',null,...at.map(r=>el('tr',null,el('td',{st:{fontWeight:'500',color:'var(--t1)'}},r.year),
...syms.map(s=>{const v=r[s];return el('td',{cl:'mn',st:{color:v==null?'var(--t3)':v>=0?'var(--gn)':'var(--rd)'}},v!=null?v.toFixed(1):'—');}),
el('td',{cl:'mn',st:{fontWeight:'600',color:r.Portfolio!=null?(r.Portfolio>=0?'var(--gn)':'var(--rd)'):'var(--t3)'}},r.Portfolio!=null?r.Portfolio.toFixed(1):'—')
))))));
}

// ── Monthly Returns Heatmap ──
const mt=data.monthly_table||[];
if(mt.length){
ct.appendChild(el('div',{cl:'cd mb fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Monthly Returns (%) — Last 36 Months')),
el('div',{st:{overflowX:'auto'}},
el('table',{cl:'dt'},
el('thead',null,el('tr',null,el('th',null,'Month'),...syms.map(s=>el('th',null,s)),el('th',{st:{color:'var(--ac)'}},'Portfolio'))),
el('tbody',null,...mt.slice(-24).map(r=>{
const pm=r.month;
return el('tr',null,el('td',{st:{fontWeight:'500',color:'var(--t1)',whiteSpace:'nowrap'}},pm),
...syms.map(s=>{const v=r[s];
const bg=v==null?'':v>3?'rgba(45,212,160,.12)':v>0?'rgba(45,212,160,.05)':v<-3?'rgba(240,96,96,.12)':v<0?'rgba(240,96,96,.05)':'';
return el('td',{cl:'mn',st:{background:bg,color:v==null?'var(--t3)':v>=0?'var(--gn)':'var(--rd)',fontSize:'8px'}},v!=null?v.toFixed(1):'—');}),
el('td',{cl:'mn',st:{fontWeight:'600',background:r.Portfolio==null?'':r.Portfolio>0?'rgba(212,151,58,.06)':'rgba(240,96,96,.06)',color:r.Portfolio!=null?(r.Portfolio>=0?'var(--gn)':'var(--rd)'):'var(--t3)',fontSize:'8px'}},r.Portfolio!=null?r.Portfolio.toFixed(1):'—')
);}))))));
}

// ── Rolling Returns ──
const rr=data.rolling_returns||{};
if(Object.keys(rr).length){
ct.appendChild(el('div',{cl:'cd mb fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Rolling Returns (Portfolio)')),el('div',{cl:'cc',st:{height:'240px'}},el('canvas',{id:'ag4'}))));
}

// ── Return Contribution ──
const contrib=data.contribution||{};
const contribKeys=Object.keys(contrib).filter(k=>k!=='_total');
if(contribKeys.length){
ct.appendChild(el('div',{cl:'g2 mb'},
el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Return Contribution by Stock')),el('div',{st:{height:'220px'}},el('canvas',{id:'ag5'}))),
el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Contribution Breakdown'),el('span',{st:{fontSize:'9px',color:'var(--t3)'}},'Total: '+(contrib._total||0).toFixed(1)+'%')),
el('table',{cl:'dt'},el('thead',null,el('tr',null,el('th',null,'Stock'),el('th',null,'Weight'),el('th',null,'Return'),el('th',null,'Contribution'))),
el('tbody',null,...contribKeys.sort((a,b)=>(contrib[b].contribution||0)-(contrib[a].contribution||0)).map(s=>{
const c=contrib[s];
return el('tr',null,
el('td',{st:{fontWeight:'500',color:'var(--t1)'}},s),
el('td',{cl:'mn'},c.weight.toFixed(1)+'%'),
el('td',{cl:'mn',st:{color:c.stock_return>=0?'var(--gn)':'var(--rd)'}},c.stock_return.toFixed(1)+'%'),
el('td',{cl:'mn',st:{fontWeight:'600',color:c.contribution>=0?'var(--gn)':'var(--rd)'}},c.contribution.toFixed(2)+'%'));
}))))
));
}

// ── Correlation Matrix ──
const corrData=data.correlation||[];
if(corrData.length){
const uLabels=[...new Set(corrData.map(c=>c.x))];
ct.appendChild(el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Correlation Matrix (Daily Returns)')),
el('table',{cl:'dt'},el('thead',null,el('tr',null,el('th',null,''),...uLabels.map(l=>el('th',{st:{fontSize:'7px'}},l)))),
el('tbody',null,...uLabels.map(row=>el('tr',null,el('td',{st:{fontWeight:'500',color:'var(--t1)',fontSize:'8px'}},row),
...uLabels.map(col=>{const entry=corrData.find(c=>c.x===row&&c.y===col);const v=entry?entry.value:0;
const bg=v>=0.99?'rgba(212,151,58,.12)':v>0.7?'rgba(240,96,96,.12)':v>0.3?'rgba(240,152,56,.06)':v<-0.3?'rgba(88,150,240,.10)':'';
return el('td',{cl:'mn',st:{background:bg,color:Math.abs(v)>0.5?'var(--t1)':'var(--t2)',fontSize:'8px'}},v.toFixed(2));})))))));
}

// ── Render Charts ──
setTimeout(()=>{
// Portfolio Growth Chart
const gr=data.growth||{};
if(gr.dates&&gr.dates.length){
const ds=[{l:'Portfolio',d:gr.portfolio.map(v=>(v*100).toFixed(1)),c:'#d4973a',fill:false}];
syms.forEach((s,i)=>{if(gr[s])ds.push({l:s,d:gr[s].map(v=>(v*100).toFixed(1)),c:SCOLORS[i%SCOLORS.length]});});
const cv=document.getElementById('ag1');
if(cv){CTS['ag1']=new Chart(cv,{type:'line',data:{labels:gr.dates.map(d=>d.slice(2)),datasets:ds.map(d=>({label:d.l,data:d.d,borderColor:d.c,backgroundColor:'transparent',borderWidth:d.l==='Portfolio'?2.5:1,pointRadius:0,tension:.3}))},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:'#586880',font:{size:8},boxWidth:8}},tooltip:{mode:'index',intersect:false,backgroundColor:'#1e2a3c',titleColor:'#e8ecf4',bodyColor:'#8d9db8',titleFont:{size:9},bodyFont:{size:9},callbacks:{label:ctx=>ctx.dataset.label+': '+(+ctx.raw).toFixed(1)+'%'}}},scales:{x:{grid:{color:'rgba(37,48,69,.2)'},ticks:{color:'#586880',font:{size:7},maxTicksLimit:10},border:{display:false}},y:{title:{display:true,text:'Cumulative Return (%)',color:'#586880',font:{size:8}},grid:{color:'rgba(37,48,69,.2)'},ticks:{color:'#586880',font:{size:7}},border:{display:false}}}}});}
}

// Asset Allocation Pie
const alloc=data.allocation||[];
if(alloc.length){
mkDoughnut('ag2',alloc.map(a=>a.symbol),alloc.map(a=>a.weight),alloc.map((_,i)=>SCOLORS[i%SCOLORS.length]));
}

// Annual Returns Bar Chart
if(at.length){
const years2=at.map(r=>r.year);
const barDS=syms.map((s,i)=>({label:s,data:at.map(r=>r[s]||0),backgroundColor:SCOLORS[i%SCOLORS.length]+'99',borderWidth:0,borderRadius:1}));
barDS.push({label:'Portfolio',data:at.map(r=>r.Portfolio||0),backgroundColor:'#d4973a',borderWidth:0,borderRadius:2});
const cv3=document.getElementById('ag3');
if(cv3){CTS['ag3']=new Chart(cv3,{type:'bar',data:{labels:years2,datasets:barDS},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:'#586880',font:{size:7},boxWidth:7}}},scales:{x:{grid:{display:false},ticks:{color:'#586880',font:{size:8}},border:{display:false}},y:{title:{display:true,text:'Return (%)',color:'#586880',font:{size:8}},grid:{color:'rgba(37,48,69,.2)'},ticks:{color:'#586880',font:{size:7}},border:{display:false}}}}});}
}

// Rolling Returns
if(Object.keys(rr).length){
const rrColors={'1M':'#5896f0','3M':'#2dd4a0','1Y':'#d4973a'};
const rrDS=Object.entries(rr).map(([label,rd])=>({label:label+' Rolling',data:rd.values,borderColor:rrColors[label]||'#888',backgroundColor:'transparent',borderWidth:label==='1Y'?2:1.2,pointRadius:0,tension:.3}));
const maxLen=Math.max(...Object.values(rr).map(r=>r.dates.length));
const longestKey=Object.keys(rr).reduce((a,b)=>rr[a].dates.length>rr[b].dates.length?a:b);
const cv4=document.getElementById('ag4');
if(cv4){CTS['ag4']=new Chart(cv4,{type:'line',data:{labels:rr[longestKey].dates.map(d=>d.slice(2)),datasets:rrDS},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:'#586880',font:{size:8},boxWidth:8}},tooltip:{mode:'index',intersect:false,backgroundColor:'#1e2a3c'}},scales:{x:{grid:{color:'rgba(37,48,69,.2)'},ticks:{color:'#586880',font:{size:7},maxTicksLimit:10},border:{display:false}},y:{title:{display:true,text:'Rolling Return (%)',color:'#586880',font:{size:8}},grid:{color:'rgba(37,48,69,.2)'},ticks:{color:'#586880',font:{size:7}},border:{display:false}}}}});}
}

// Return Contribution Bar Chart
if(contribKeys.length){
const sortedCK=contribKeys.sort((a,b)=>(contrib[b].contribution||0)-(contrib[a].contribution||0));
const cv5=document.getElementById('ag5');
if(cv5){CTS['ag5']=new Chart(cv5,{type:'bar',data:{labels:sortedCK,datasets:[{label:'Contribution (%)',data:sortedCK.map(k=>contrib[k].contribution),backgroundColor:sortedCK.map(k=>contrib[k].contribution>=0?'rgba(45,212,160,.6)':'rgba(240,96,96,.6)'),borderWidth:0,borderRadius:2}]},options:{responsive:true,maintainAspectRatio:false,indexAxis:'horizontal',plugins:{legend:{display:false}},scales:{x:{grid:{display:false},ticks:{color:'#586880',font:{size:7}},border:{display:false}},y:{title:{display:true,text:'Return Contribution (%)',color:'#586880',font:{size:8}},grid:{color:'rgba(37,48,69,.2)'},ticks:{color:'#586880',font:{size:7}},border:{display:false}}}}});}
}

},80);

}catch(e){
ct.innerHTML='<div class="ld" style="color:var(--rd)">Analytics error: '+e.message+'</div>';
console.error('Analytics:',e);
}
}

// === RISK CORE ===
function vRisk(ct){
const rc=DT.risk_core||{},p=rc.portfolio||{},ms=rc.market_summary||{};
ct.appendChild(el('div',{cl:'g4 mb'},kpi('Mean Vol',p.mean_vol_21d||0,'%','var(--bl)','21d'),kpi('Mean VaR',p.mean_var||0,'%','var(--rd)','Hist 5%'),kpi('Mean CVaR',p.mean_cvar||0,'%','var(--rd)','Exp Shortfall'),kpi('Worst DD',p.worst_dd||0,'%','var(--or)','Peak-Trough')));
ct.appendChild(el('div',{cl:'cd mb fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Market Risk Metrics (InteliRisk v4: HAR-RV + EVT/GPD + Cornish-Fisher)')),
el('table',{cl:'dt'},el('thead',null,el('tr',null,...['Market','Vol 21d','Vol 63d','HAR fc','R²','VaR 5%','CVaR','EVT VaR','MaxDD','JB Non-Norm'].map(t=>el('th',null,t)))),
el('tbody',null,...Object.entries(ms).map(([m,d])=>el('tr',null,el('td',{st:{fontWeight:'500',color:'var(--t1)'}},d.name||m),el('td',{cl:'mn',st:{color:'var(--bl)'}},fmtPct(d.vol_21d)),el('td',{cl:'mn'},fmtPct(d.vol_63d)),el('td',{cl:'mn',st:{color:'var(--pu)'}},fmtPct(d.har_forecast)),el('td',{cl:'mn'},fmt(d.har_r2,3)),el('td',{cl:'mn',st:{color:'var(--rd)'}},fmt(d.current_var,3)),el('td',{cl:'mn',st:{color:'var(--rd)'}},fmt(d.current_cvar,3)),el('td',{cl:'mn'},d.evt_var!=null?fmt(d.evt_var,3):'—'),el('td',{cl:'mn',st:{color:'var(--or)'}},fmtPct(d.max_dd)),el('td',null,d.non_normal?'✓ Fat tails':'Normal')))))));
ct.appendChild(el('div',{cl:'g2'},el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Component VaR')),el('div',{cl:'cc'},el('canvas',{id:'rk1'}))),el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Return Distribution')),el('div',{cl:'cc'},el('canvas',{id:'rk2'})))));
setTimeout(()=>{const cv=rc.component_var||{};if(Object.keys(cv).length)mkBar('rk1',Object.keys(cv).map(k=>(DT.risk_core.market_summary[k]||{}).short||k),Object.values(cv),Object.keys(cv).map(k=>({JSE_SA:'#1F4E79'}[k]||'#888')));const ts=rc.time_series||{};if(ts.returns&&ts.returns.length){const bins={};ts.returns.forEach(r=>{const b=(Math.round(r*10)/10).toFixed(1);bins[b]=(bins[b]||0)+1;});const sorted=Object.entries(bins).sort((a,b)=>parseFloat(a[0])-parseFloat(b[0]));mkBar('rk2',sorted.map(s=>s[0]),sorted.map(s=>s[1]),sorted.map(s=>parseFloat(s[0])<0?'rgba(240,96,96,.6)':'rgba(45,212,160,.6)'));}},60);}

// === STRUCTURAL ===
function vStructural(ct){
const s=DT.structural||{},eg=s.eigen_concentration||{},co=s.correlation||{},pc=s.pca||{},nt=s.network||{};
const pcLabels=pc.labels||[];
const dominantFactor=pcLabels[0]||'PC1';
const nAssets=s.n_assets||1;

// KPI Row
ct.appendChild(el('div',{cl:'g5 mb'},
kpi(dominantFactor,((pc.pc1_explained||0)*100).toFixed(1),'%','var(--pu)','PC1 of '+nAssets+' assets'),
kpi('Eigen Conc',((eg.current||0)*100).toFixed(1),'%',eg.fragile?'var(--rd)':'var(--gn)','Threshold: 60%'),
kpi('Avg Corr',((co.avg||0)*100).toFixed(1),'%','var(--bl)',nAssets+' stocks, 252d'),
kpi('Fragile',eg.fragile?'YES':'NO','',eg.fragile?'var(--rd)':'var(--gn)',eg.interpretation||''),
kpi('Network',nt.nodes?nt.nodes.length:0,'nodes','var(--cy)','Density: '+(nt.density||0).toFixed(2))
));

// Row 1: PC1 Concentration + Avg Correlation
ct.appendChild(el('div',{cl:'g2 mb'},
el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'PC1 Concentration (0.60 = fragility)')),el('div',{cl:'cc cc-t'},el('canvas',{id:'s1'}))),
el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Avg Pairwise Correlation (Rolling 60d)')),el('div',{cl:'cc cc-t'},el('canvas',{id:'s2'})))
));

// Row 2: Eigenvalue Scree + Cumulative Variance
ct.appendChild(el('div',{cl:'g2 mb'},
el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Eigenvalue Scree Plot')),el('div',{cl:'cc cc-t'},el('canvas',{id:'s3'}))),
el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Cumulative Variance Explained')),el('div',{cl:'cc cc-t'},el('canvas',{id:'s3b'})))
));

// PCA Decomposition Table
const scree=pc.scree||[];
if(scree.length){
ct.appendChild(el('div',{cl:'cd mb fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'PCA Decomposition — Factor Breakdown')),
el('table',{cl:'dt'},el('thead',null,el('tr',null,...['Factor','Eigenvalue','Variance %','Cumulative %','Top Loadings'].map(t=>el('th',null,t)))),
el('tbody',null,...scree.map((r,i)=>{
const topL=(pc.top_loadings||{})[r.pc]||[];
return el('tr',null,
el('td',{st:{fontWeight:'600',color:i===0?'var(--pu)':'var(--t1)'}},r.pc),
el('td',{cl:'mn'},r.eigenvalue.toFixed(2)),
el('td',{cl:'mn',st:{color:'var(--pu)'}},r.variance_pct.toFixed(1)+'%'),
el('td',{cl:'mn'},r.cumulative_pct.toFixed(1)+'%'),
el('td',{st:{fontSize:'8px',color:'var(--t3)'}},topL.slice(0,3).map(l=>l.asset+'('+l.loading.toFixed(2)+')').join(', '))
);})
))));
}

// PC Time Series
const pcts=pc.pc_time_series||{};
if(pcts.dates&&pcts.dates.length){
ct.appendChild(el('div',{cl:'cd mb fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Principal Component Time Series')),el('div',{cl:'cc cc-t'},el('canvas',{id:'s5'}))));
}

// Factor Loadings Heatmap
const ldgs=pc.loadings||[];
if(ldgs.length){
const assets=[...new Set(ldgs.map(l=>l.market))];
const comps=[...new Set(ldgs.map(l=>l.component))].slice(0,5);
ct.appendChild(el('div',{cl:'cd mb fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Factor Loadings Heatmap (Assets x PCs)')),
el('div',{st:{overflowX:'auto'}},
el('table',{cl:'dt'},el('thead',null,el('tr',null,el('th',null,'Asset'),...comps.map(c=>el('th',null,c)))),
el('tbody',null,...assets.map(a=>el('tr',null,el('td',{st:{fontWeight:'500',color:'var(--t1)',fontSize:'8px'}},a),
...comps.map(c=>{const entry=ldgs.find(l=>l.market===a&&l.component===c);const v=entry?entry.value:0;
const bg=v>0.3?'rgba(152,120,232,.15)':v<-0.3?'rgba(240,96,96,.12)':'';
return el('td',{cl:'mn',st:{background:bg,color:Math.abs(v)>0.2?'var(--t1)':'var(--t3)',fontSize:'8px'}},v.toFixed(2));})
)))))));
}

// Correlation Heatmap
const corrMatrix=co.matrix||[];
if(corrMatrix.length){
const uniqueLabels=[...new Set(corrMatrix.map(c=>c.x))];
ct.appendChild(el('div',{cl:'cd mb fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Correlation Heatmap ('+uniqueLabels.length+' assets)')),
el('div',{st:{overflowX:'auto',maxHeight:'400px',overflowY:'auto'}},
el('table',{cl:'dt'},el('thead',null,el('tr',null,el('th',null,''),...uniqueLabels.map(l=>el('th',{st:{fontSize:'7px',writingMode:'vertical-rl',transform:'rotate(180deg)',height:'60px'}},l)))),
el('tbody',null,...uniqueLabels.map(row=>el('tr',null,el('td',{st:{fontWeight:'500',color:'var(--t1)',fontSize:'7px',whiteSpace:'nowrap'}},row),
...uniqueLabels.map(col=>{const entry=corrMatrix.find(c=>c.x===row&&c.y===col);const v=entry?entry.value:0;
const bg=v>=0.99?'rgba(212,151,58,.1)':v>0.7?'rgba(240,96,96,.15)':v>0.4?'rgba(240,152,56,.08)':v<-0.2?'rgba(88,150,240,.12)':'';
return el('td',{cl:'mn',st:{background:bg,color:Math.abs(v)>0.4?'var(--t1)':'var(--t3)',fontSize:'7px',padding:'2px 3px'}},v.toFixed(2));})))))));
}

// Contagion Network
ct.appendChild(el('div',{cl:'cd mb fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Contagion Network — '+((nt.nodes||[]).length)+' nodes, '+((nt.edges||[]).length)+' edges'),
el('span',{st:{fontSize:'8px',color:'var(--t3)'}},'Clustering: '+(nt.clustering||0).toFixed(2))),
el('div',{cl:'nv',id:'netv',st:{height:'400px'}})));

// Factor Boxplot (from Layer 3 factors)
ct.appendChild(el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Factor Distribution (Boxplot)')),el('div',{cl:'cc cc-t'},el('canvas',{id:'s4'}))));

setTimeout(()=>{
// PC1 Concentration chart
const eh=eg.history||[];
if(eh.length)mkLine('s1',eh.map(h=>h.date.slice(5)),[{l:dominantFactor,d:eh.map(h=>h.pc1),c:'#9878e8'},{l:dominantFactor+' + '+(pcLabels[1]||'PC2'),d:eh.map(h=>h.pc12),c:'#5896f0'}]);

// Avg Correlation chart
const ch=co.history||[];
if(ch.length)mkLine('s2',ch.map(h=>h.date.slice(5)),[{l:'Avg Corr',d:ch.map(h=>h.value),c:'#5896f0'}]);

// Eigenvalue Scree bar chart
if(scree.length){
mkBar('s3',scree.map(s=>s.pc),scree.map(s=>s.eigenvalue),scree.map((_,i)=>i===0?'rgba(152,120,232,.7)':'rgba(152,120,232,.3)'));
// Cumulative variance line
const cv3b=document.getElementById('s3b');
if(cv3b){CTS['s3b']=new Chart(cv3b,{type:'line',data:{labels:scree.map(s=>s.pc),datasets:[
{label:'Cumulative %',data:scree.map(s=>s.cumulative_pct),borderColor:'#d4973a',backgroundColor:'rgba(212,151,58,.08)',borderWidth:2,pointRadius:3,pointBackgroundColor:'#d4973a',fill:true,tension:.3},
{label:'Individual %',data:scree.map(s=>s.variance_pct),borderColor:'#9878e8',backgroundColor:'transparent',borderWidth:1.5,pointRadius:2,tension:.3}
]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:'#586880',font:{size:8},boxWidth:8}}},scales:{x:{grid:{display:false},ticks:{color:'#586880',font:{size:7}},border:{display:false}},y:{title:{display:true,text:'Variance %',color:'#586880',font:{size:8}},grid:{color:'rgba(37,48,69,.2)'},ticks:{color:'#586880',font:{size:7}},border:{display:false}}}}});}
}

// PC Time Series
if(pcts.dates&&pcts.dates.length){
const pcColors=['#9878e8','#5896f0','#2dd4a0','#f06060','#d4973a'];
const pcDS=pcLabels.slice(0,5).filter(l=>pcts[l]).map((l,i)=>({l,d:pcts[l],c:pcColors[i%pcColors.length]}));
mkLine('s5',pcts.dates.map(d=>d.slice(2)),pcDS);
}

// Boxplot (Layer 3 factors)
const fts=(DT.factors||{}).factor_time_series||[];
const fnames=(DT.factors||{}).factor_names||[];
if(fts.length&&fnames.length){
const boxData=fnames.map(fn=>{const vals=fts.map(r=>r[fn]||0).filter(v=>!isNaN(v));vals.sort((a,b)=>a-b);const q1=vals[Math.floor(vals.length*0.25)]||0;const med=vals[Math.floor(vals.length*0.5)]||0;const q3=vals[Math.floor(vals.length*0.75)]||0;return{label:((DT.factors||{}).factor_meta||{})[fn]?.label||fn,q1,med,q3};});
const cv4=document.getElementById('s4');if(cv4){
CTS['s4']=new Chart(cv4,{type:'bar',data:{labels:boxData.map(b=>b.label),datasets:[
{label:'IQR',data:boxData.map(b=>b.q3-b.q1),backgroundColor:'rgba(152,120,232,.3)',borderColor:'#9878e8',borderWidth:1,borderRadius:2},
{label:'Median',data:boxData.map(b=>b.med),type:'line',borderColor:'#d4973a',backgroundColor:'transparent',borderWidth:2,pointRadius:4,pointBackgroundColor:'#d4973a'}
]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:'#586880',font:{size:8},boxWidth:8}}},scales:{x:{grid:{display:false},ticks:{color:'#586880',font:{size:7}},border:{display:false}},y:{grid:{color:'rgba(37,48,69,.2)'},ticks:{color:'#586880',font:{size:7}},border:{display:false}}}}});
}}

// D3 Contagion Network
const ctr=document.getElementById('netv');if(ctr&&nt.nodes&&nt.nodes.length>1){
const w=ctr.clientWidth,ht=ctr.clientHeight;
const svg=d3.select(ctr).append('svg').attr('width',w).attr('height',ht);
const g=svg.append('g');
svg.call(d3.zoom().scaleExtent([.3,3]).on('zoom',e=>g.attr('transform',e.transform)));
const nodes=nt.nodes.map(n=>({...n}));
const edges=(nt.edges||[]).map(e=>({source:nodes.find(n=>n.short===e.source),target:nodes.find(n=>n.short===e.target),weight:e.weight})).filter(e=>e.source&&e.target);
const chargeStr=nodes.length>15?-120:-200;
const linkDist=nodes.length>15?60:100;
const sim=d3.forceSimulation(nodes).force('link',d3.forceLink(edges).distance(linkDist)).force('charge',d3.forceManyBody().strength(chargeStr)).force('center',d3.forceCenter(w/2,ht/2)).force('collision',d3.forceCollide().radius(d=>6+d.eigenvector*25));
g.selectAll('line').data(edges).enter().append('line').attr('stroke','rgba(88,150,240,.12)').attr('stroke-width',d=>Math.max(0.5,d.weight*3));
const node=g.selectAll('circle').data(nodes).enter().append('circle').attr('r',d=>5+d.eigenvector*25).attr('fill',d=>d.color||'#888').attr('opacity',0.8).call(d3.drag().on('start',(e,d)=>{if(!e.active)sim.alphaTarget(.3).restart();d.fx=d.x;d.fy=d.y;}).on('drag',(e,d)=>{d.fx=e.x;d.fy=e.y;}).on('end',(e,d)=>{if(!e.active)sim.alphaTarget(0);d.fx=null;d.fy=null;}));
g.selectAll('text').data(nodes).enter().append('text').text(d=>d.short).attr('fill','#8d9db8').attr('font-size','8px').attr('text-anchor','middle').attr('dy',d=>-(8+d.eigenvector*25));
const link=g.selectAll('line'),lbl=g.selectAll('text');
sim.on('tick',()=>{link.attr('x1',d=>d.source.x).attr('y1',d=>d.source.y).attr('x2',d=>d.target.x).attr('y2',d=>d.target.y);node.attr('cx',d=>d.x).attr('cy',d=>d.y);lbl.attr('x',d=>d.x).attr('y',d=>d.y);});
}
},60);}

// === FACTORS ===
function vFactors(ct){
const f=DT.factors||{},zs=f.current_zscores||{},dom=f.dominant_factor||{};
ct.appendChild(el('div',{cl:'g4 mb'},kpi('Dominant',dom.label||'—','',dom.zscore>1.5?'var(--rd)':'var(--bl)','z='+dom.zscore),kpi('Shock',f.shock_classification?.current||'—','','var(--or)',f.shock_classification?.types?.[f.shock_classification?.current]||''),kpi('Herding',f.herding?.level||'N/A','',f.herding?.sig?'var(--rd)':'var(--gn)','CSAD γ₂<0'),kpi('R² (JSE)',((f.r_squared||{}).JSE_SA||0)*100|0,'%','var(--ac)','Factor model fit')));
ct.appendChild(el('div',{cl:'cd mb fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Factor Z-Scores (6 factors: Global/Domestic/Behavioral)')),el('div',{cl:'g3',st:{gap:'7px'}},...Object.entries(zs).map(([n,z])=>{const meta=(f.factor_meta||{})[n]||{};const zv=z??0;const c=Math.abs(zv)>2?'var(--rd)':Math.abs(zv)>1?'var(--or)':'var(--gn)';return el('div',{st:{background:'var(--b0)',borderRadius:'5px',padding:'9px'}},el('div',{st:{fontSize:'8px',color:meta.color||'var(--t3)',textTransform:'uppercase',letterSpacing:'.4px',marginBottom:'3px'}},meta.label||n),el('div',{st:{fontFamily:'JetBrains Mono',fontSize:'16px',fontWeight:'600',color:c}},zv.toFixed(2)),bar(Math.abs(zv),3,c),el('div',{st:{fontSize:'7px',color:'var(--t3)',marginTop:'2px'}},meta.cls||''));}))));
ct.appendChild(el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Factor Evolution')),el('div',{cl:'cc cc-t'},el('canvas',{id:'f1'}))));
setTimeout(()=>{const fts=f.factor_time_series||[];if(fts.length){const fnames=f.factor_names||[];const cl=['#1F4E79','#2E75B6','#E67E22','#C0392B','#8E44AD','#C9A227'];mkLine('f1',fts.map(r=>(r.date||'').slice(5)),fnames.map((fn,i)=>({l:(f.factor_meta||{})[fn]?.label||fn,d:fts.map(r=>r[fn]||0),c:cl[i%cl.length]})));}},60);}

// === REGIME ===
const REGIME_DESC={
'Stable Growth':'Low volatility, positive returns. Equity markets trending upward with contained risk. Diversification works well.',
'Commodity Expansion':'Commodity prices driving markets. Mining and resource sectors outperform. ZAR typically strengthens with commodity prices.',
'USD Tightening':'USD strength pressuring EM currencies. Capital outflows from emerging markets. ZAR weakens, imported inflation risk rises.',
'Sovereign Stress':'Elevated sovereign risk premiums. CDS spreads widening, bond yields rising. Political or fiscal uncertainty dominates.',
'Systemic Crisis':'Extreme volatility, correlation spike, drawdowns. All asset correlations converge to 1. Diversification fails. Risk-off mode.'
};
function vRegime(ct){
const r=DT.regime||{},fc=r.forecast||{};
// KPI row — removed LL, added regime description
ct.appendChild(el('div',{cl:'g4 mb'},kpi('Active',r.active_regime||'—','',RCLR[r.active_regime],'Baum-Welch HMM'),kpi('Crisis',((r.crisis_probability||0)*100).toFixed(0),'%',r.crisis_probability>0.3?'var(--rd)':'var(--gn)','Sov+Crisis prob'),kpi('+1D Forecast',fc['+1d']?.regime||'—','',RCLR[fc['+1d']?.regime],'1-day ahead'),kpi('+21D Forecast',fc['+21d']?.regime||'—','',RCLR[fc['+21d']?.regime],'1-month ahead')));

// Regime Distribution with description on hover
ct.appendChild(el('div',{cl:'cd mb fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Regime Distribution')),
el('div',{cl:'rb'},...Object.entries(r.regime_probs||{}).filter(([_,p])=>p>.01).map(([n,p])=>el('div',{cl:'rseg',st:{width:(p*100)+'%',background:RCLR[n]||'#888',cursor:'pointer'},title:n+': '+REGIME_DESC[n]||''},p>.08?n.split(' ')[0]+' '+(p*100).toFixed(0)+'%':''))),
// Regime characteristics below distribution
el('div',{st:{marginTop:'10px',padding:'10px',background:'var(--b0)',borderRadius:'6px',border:'1px solid var(--bd)'}},
el('div',{st:{fontSize:'10px',fontWeight:'600',color:RCLR[r.active_regime]||'var(--ac)',marginBottom:'4px'}},r.active_regime||'—'),
el('div',{st:{fontSize:'10px',color:'var(--t2)',lineHeight:'1.5'}},REGIME_DESC[r.active_regime]||'No description available.'))
));

// Individual regime cards with descriptions
ct.appendChild(el('div',{cl:'g5 mb',st:{gap:'6px'}},...Object.entries(r.regime_probs||{}).sort((a,b)=>b[1]-a[1]).map(([name,prob])=>{
const isActive=name===r.active_regime;
return el('div',{st:{background:isActive?'rgba(212,151,58,.06)':'var(--b0)',border:'1px solid '+(isActive?RCLR[name]||'var(--ac)':'var(--bd)'),borderRadius:'6px',padding:'8px',cursor:'pointer'},
title:REGIME_DESC[name]||''},
el('div',{st:{fontSize:'8px',color:RCLR[name]||'var(--t3)',fontWeight:'600',textTransform:'uppercase',letterSpacing:'.3px',marginBottom:'3px'}},name),
el('div',{cl:'mn',st:{fontSize:'14px',fontWeight:'600',color:isActive?RCLR[name]:'var(--t2)'}},(prob*100).toFixed(1)+'%'),
bar(prob*100,100,RCLR[name]||'var(--ac)'),
el('div',{st:{fontSize:'7px',color:'var(--t3)',marginTop:'4px',lineHeight:'1.3'}},(REGIME_DESC[name]||'').slice(0,60)+'...')
);})));

ct.appendChild(el('div',{cl:'cd mb fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Regime Probability History')),el('div',{cl:'cc cc-t'},el('canvas',{id:'rg1'}))));

// Transition Matrix with interpretation
const tm=r.transition_matrix||{};const nms=Object.keys(tm);
if(nms.length){
// Find highest off-diagonal transition
let maxTrans={from:'',to:'',val:0};
nms.forEach(f2=>nms.forEach(t2=>{if(f2!==t2){const v=(tm[f2]||{})[t2]||0;if(v>maxTrans.val)maxTrans={from:f2,to:t2,val:v};}}));
ct.appendChild(el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Transition Matrix')),
el('div',{st:{fontSize:'9px',color:'var(--t2)',marginBottom:'8px',lineHeight:'1.5'}},
'Shows probability of moving from one regime to another. Diagonal = persistence. High diagonal values indicate stable regimes.'+
(maxTrans.val>0.05?' Most likely transition: '+maxTrans.from.split(' ')[0]+' → '+maxTrans.to.split(' ')[0]+' ('+(maxTrans.val*100).toFixed(1)+'%)':'')),
el('table',{cl:'dt'},el('thead',null,el('tr',null,el('th',null,'From → To'),...nms.map(n=>el('th',{st:{color:RCLR[n]||'var(--t3)'}},n.split(' ')[0])))),
el('tbody',null,...nms.map(f2=>el('tr',null,el('td',{st:{fontWeight:'500',fontSize:'8px',color:RCLR[f2]||'var(--t1)'}},f2),...nms.map(t2=>{
const v=(tm[f2]||{})[t2]||0;const isDiag=f2===t2;
const bg=isDiag?(v>.9?'rgba(45,212,160,.08)':'rgba(212,151,58,.06)'):(v>.1?'rgba(240,96,96,.08)':v>.05?'rgba(212,151,58,.03)':'');
return el('td',{cl:'mn',st:{background:bg,color:isDiag?'var(--gn)':v>.1?'var(--or)':'var(--t2)',fontWeight:isDiag||v>.1?'600':'400'}},v.toFixed(2));
}))))),
el('div',{st:{fontSize:'8px',color:'var(--t3)',marginTop:'6px',fontStyle:'italic'}},'Diagonal values (green) show regime persistence. Off-diagonal values show transition risk. Higher off-diagonal = more volatile regime switching.')
));}
setTimeout(()=>{const hi=r.history||[];if(hi.length){const nms2=Object.values(r.regime_labels||{});const cl=['rgba(30,107,46,.45)','rgba(46,117,182,.45)','rgba(201,162,39,.45)','rgba(230,126,34,.45)','rgba(192,57,43,.45)'];mkStacked('rg1',hi.map(h=>h.date.slice(5)),nms2.map((n,i)=>({l:n,d:hi.map(h=>h[n]||0),c:cl[i]})));}},60);}

// === STRESS SIMULATOR ===
let SIM={baseline:null,scenarios:{},currentScenario:null,currentPath:'Representative',viewMode:'Detailed View'};

function vStress(ct){
// Two-column layout: left=Scenario Builder, right=Results
ct.appendChild(el('div',{cl:'g2 mb',st:{gap:'12px'}},
// LEFT: Scenario Builder
el('div',{cl:'cd fi'},
el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Scenario Builder')),
// Scenario Name
el('div',{st:{marginBottom:'10px'}},
el('label',{cl:'fl'},'Scenario Name'),
el('input',{cl:'fin',id:'sim-name',type:'text',value:'Scenario '+(Object.keys(SIM.scenarios).length+1)})),
// Simulation Method
el('div',{st:{marginBottom:'10px'}},
el('label',{cl:'fl'},'Simulation Method'),
el('div',{st:{display:'flex',gap:'6px'}},
el('label',{st:{fontSize:'10px',display:'flex',alignItems:'center',gap:'4px',cursor:'pointer',color:'var(--t2)'}},el('input',{type:'radio',name:'sim-method',value:'mc',checked:'checked',st:{accentColor:'var(--ac)'}}), 'Monte Carlo'),
el('label',{st:{fontSize:'10px',display:'flex',alignItems:'center',gap:'4px',cursor:'pointer',color:'var(--t2)'}},el('input',{type:'radio',name:'sim-method',value:'hr',st:{accentColor:'var(--ac)'}}), 'Historical Replay'))),
// MC Options
el('div',{id:'mc-opts'},
el('div',{cl:'g2',st:{gap:'8px',marginBottom:'8px'}},
el('div',null,el('label',{cl:'fl'},'Simulations'),el('input',{cl:'fin',id:'sim-nsims',type:'number',value:'500',min:'100',max:'2000',step:'100'})),
el('div',null,el('label',{cl:'fl'},'Days'),el('input',{cl:'fin',id:'sim-ndays',type:'number',value:'250',min:'25',max:'1000',step:'25'}))),
el('div',{st:{marginBottom:'8px'}},
el('label',{cl:'fl'},'Volatility Regime (HMM)'),
el('select',{cl:'fin',id:'sim-regime'},el('option',{value:'Low'},'Low'),el('option',{value:'Medium',selected:'selected'},'Medium'),el('option',{value:'High'},'High'))),
el('div',{st:{marginBottom:'8px'}},
el('label',{cl:'fl'},'Stress Multiplier'),
el('select',{cl:'fin',id:'sim-level'},
el('option',{value:'1.0x'},'1.0x (Normal)'),el('option',{value:'1.15x'},'1.15x'),
el('option',{value:'1.25x'},'1.25x'),el('option',{value:'1.35x'},'1.35x'),
el('option',{value:'1.5x'},'1.5x (Extreme)')))),
// HR Options (hidden by default)
el('div',{id:'hr-opts',st:{display:'none'}},
el('label',{cl:'fl'},'Historical Crisis'),
el('select',{cl:'fin',id:'sim-crisis'},
el('option',{value:'COVID-19 Crash'},'COVID-19 Crash (Feb-Apr 2020)'),
el('option',{value:'Global Financial Crisis'},'Global Financial Crisis (2007-2009)'),
el('option',{value:'Dot-Com Bubble'},'Dot-Com Bubble (2000-2002)'),
el('option',{value:'2011 Eurozone Crisis'},'2011 Eurozone Crisis'),
el('option',{value:'2022 Inflation Crash'},'2022 Inflation Crash'),
el('option',{value:'Taper Tantrum (2013)'},'Taper Tantrum (2013)'),
el('option',{value:'China Slowdown (2015)'},'China Slowdown (2015)'),
el('option',{value:'Commodity Crash (2014)'},'Commodity Crash (2014-2016)'))),
// Factor Stress (optional)
el('div',{st:{marginTop:'12px',borderTop:'1px solid var(--bd)',paddingTop:'10px'}},
el('label',{st:{fontSize:'9px',display:'flex',alignItems:'center',gap:'5px',cursor:'pointer',color:'var(--t2)',marginBottom:'8px'}},
el('input',{type:'checkbox',id:'sim-use-factors',st:{accentColor:'var(--ac)'},onChange:function(){$('#factor-panel').style.display=this.checked?'block':'none';}}),
'Apply Factor Stress (Fama-French)'),
el('div',{id:'factor-panel',st:{display:'none'}},
el('div',{cl:'g2',st:{gap:'6px',marginBottom:'8px'}},
...['Mkt-RF','SMB','HML','RMW','CMA','Mom'].map(f=>
el('div',{st:{background:'var(--b0)',borderRadius:'4px',padding:'6px'}},
el('div',{st:{fontSize:'7px',color:'var(--t3)',textTransform:'uppercase',marginBottom:'3px'}},f),
el('input',{cl:'fin',type:'range',min:'-50',max:'50',value:'0',step:'5',id:'shock-'+f,st:{width:'100%',padding:'2px'},onChange:function(){$('#shk-val-'+f).textContent=this.value+'%';}}),
el('div',{st:{fontSize:'8px',color:'var(--ac)',textAlign:'center'},id:'shk-val-'+f},'0%')))),
)),
// Run button
el('button',{cl:'btn',st:{marginTop:'12px'},id:'sim-run-btn',onClick:runSimulation},'Run Simulation'),
el('div',{id:'sim-status',st:{fontSize:'9px',color:'var(--t3)',marginTop:'6px',minHeight:'14px'}})
),
// RIGHT: Saved Scenarios
el('div',{cl:'cd fi'},
el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Scenarios'),el('button',{cl:'tbtn',onClick:async function(){if(!PID)return;try{const r=await api('/api/v1/simulator/'+PID+'/baseline',{method:'POST',body:JSON.stringify({weight_method:'eq'})});SIM.baseline=r.baseline;renderSimResults();}catch(e){console.error(e);}}},'Init Baseline')),
el('div',{id:'sim-scenario-list'},el('div',{st:{fontSize:'10px',color:'var(--t3)',padding:'16px',textAlign:'center'}},'Click "Init Baseline" to start, then build scenarios.'))
)
));
// Radio toggle
document.querySelectorAll('input[name="sim-method"]').forEach(r=>r.addEventListener('change',function(){
$('#mc-opts').style.display=this.value==='mc'?'block':'none';
$('#hr-opts').style.display=this.value==='hr'?'block':'none';
}));
// Results area below
ct.appendChild(el('div',{id:'sim-results'}));
// Auto-init baseline
if(!SIM.baseline&&PID){
api('/api/v1/simulator/'+PID+'/baseline',{method:'POST',body:JSON.stringify({weight_method:'eq'})}).then(r=>{SIM.baseline=r.baseline;renderSimResults();}).catch(e=>console.error(e));
}else{renderSimResults();}
}

async function runSimulation(){
const btn=$('#sim-run-btn'),status=$('#sim-status');
if(!btn||!PID)return;
btn.disabled=true;btn.textContent='Running...';
if(status)status.textContent='Simulating...';
const name=($('#sim-name')||{}).value||'Scenario';
const method=document.querySelector('input[name="sim-method"]:checked').value;
try{
let result;
if(method==='mc'){
const body={scenario_name:name,n_sims:+($('#sim-nsims')||{}).value||500,n_days:+($('#sim-ndays')||{}).value||250,regime:($('#sim-regime')||{}).value||'Medium',stress_level:($('#sim-level')||{}).value||'1.0x'};
// Factor shocks
const useFac=$('#sim-use-factors');
if(useFac&&useFac.checked){
body.factors=['Mkt-RF','SMB','HML','RMW','CMA','Mom'];
body.shocks={};
['Mkt-RF','SMB','HML','RMW','CMA','Mom'].forEach(f=>{const v=$('#shock-'+f);if(v)body.shocks[f]=+v.value/100;});
}
result=await api('/api/v1/simulator/'+PID+'/monte-carlo',{method:'POST',body:JSON.stringify(body)});
}else{
const body={scenario_name:name,crisis:($('#sim-crisis')||{}).value||'COVID-19 Crash'};
result=await api('/api/v1/simulator/'+PID+'/historical',{method:'POST',body:JSON.stringify(body)});
}
SIM.scenarios[name]=result;
SIM.currentScenario=name;
if(status)status.textContent='Done!';
// Update scenario name input
const ni=$('#sim-name');if(ni)ni.value='Scenario '+(Object.keys(SIM.scenarios).length+1);
renderSimResults();
}catch(e){if(status)status.textContent='Error: '+e.message;console.error(e);}
btn.disabled=false;btn.textContent='Run Simulation';
}

function renderSimResults(){
const container=$('#sim-results');if(!container)return;
container.innerHTML='';
const scList=$('#sim-scenario-list');
if(scList){
scList.innerHTML='';
const names=Object.keys(SIM.scenarios);
if(names.length===0){
scList.appendChild(el('div',{st:{fontSize:'10px',color:'var(--t3)',padding:'16px',textAlign:'center'}},'No scenarios yet. Configure and run a simulation.'));
}else{
names.forEach(n=>{
const s=SIM.scenarios[n];
const isCurrent=n===SIM.currentScenario;
const isMC=s.type==='Monte Carlo';
const rep=isMC?s.paths.representative.metrics:s.metrics;
const finalMult=isMC?s.final_value_stats.mean:s.final_value_multiplier;
const pv=s.portfolio_value||100000;
const finalVal=Math.round(pv*finalMult);
const pnl=finalVal-pv;
const pnlColor=pnl>=0?'var(--gn)':'var(--rd)';
scList.appendChild(el('div',{st:{background:isCurrent?'rgba(212,151,58,.06)':'var(--b0)',border:'1px solid '+(isCurrent?'var(--ac)':'var(--bd)'),borderRadius:'6px',padding:'10px',marginBottom:'6px',cursor:'pointer',transition:'all .15s'},onClick:function(){SIM.currentScenario=n;renderSimResults();}},
el('div',{st:{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:'4px'}},
el('span',{st:{fontSize:'10px',fontWeight:'600',color:'var(--t1)'}},n),
el('span',{st:{fontSize:'7px',padding:'2px 5px',borderRadius:'3px',background:isMC?'rgba(88,150,240,.12)':'rgba(240,152,56,.12)',color:isMC?'var(--bl)':'var(--or)',fontWeight:'600'}},s.type)),
el('div',{st:{display:'flex',justifyContent:'space-between',alignItems:'baseline',marginTop:'2px'}},
el('span',{st:{fontSize:'8px',color:'var(--t3)'}},isMC?(s.regime+' / '+s.stress_level):s.crisis),
el('span',{st:{fontFamily:'JetBrains Mono',fontSize:'12px',fontWeight:'600',color:pnlColor}},(pnl>=0?'+':'')+pnl.toLocaleString()+' ZAR'))
));});
}}

if(!SIM.currentScenario||!SIM.scenarios[SIM.currentScenario])return;
const sc=SIM.scenarios[SIM.currentScenario];
const isMC=sc.type==='Monte Carlo';
const rep=isMC?sc.paths.representative.metrics:sc.metrics;
const bl=sc.baseline_metrics||{};
const pv=sc.portfolio_value||100000;

// KPI cards with delta
function metricCard(label,key,inv,unit){
const v=rep[key];const bv=bl[key];
const delta=sc.baseline_delta?sc.baseline_delta[key]:null;
const dc=delta!=null?(inv?(delta<=0?'var(--gn)':'var(--rd)'):(delta>=0?'var(--gn)':'var(--rd)')):'var(--t3)';
return el('div',{cl:'cd fi',st:{padding:'10px'}},
el('div',{cl:'cd-t',st:{marginBottom:'4px',fontSize:'8px'}},label),
el('div',{st:{display:'flex',alignItems:'baseline',gap:'4px'}},
el('span',{cl:'cv',st:{fontSize:'18px'}},v!=null?(unit==='%'?(v*100).toFixed(2)+'%':v.toFixed(4)):'—'),
delta!=null?el('span',{st:{fontSize:'8px',color:dc,fontFamily:'JetBrains Mono'}},(delta>=0?'+':'')+delta.toFixed(4)):null),
bv!=null?el('div',{st:{fontSize:'7px',color:'var(--t3)',marginTop:'2px'}},'Baseline: '+(unit==='%'?(bv*100).toFixed(2)+'%':bv.toFixed(4))):null);
}

container.appendChild(el('div',{cl:'g5 mb',st:{gap:'8px'}},
metricCard('Annual Volatility','annual_volatility',true,'%'),
metricCard('Expected Return','expected_return',false,'%'),
metricCard('Sharpe Ratio','sharpe_ratio',false),
metricCard('95% VaR','var_95',true),
metricCard('Max Drawdown','max_drawdown',true,'%')
));

// Final Portfolio Value — show ZAR and USD
const finalMult=isMC?sc.final_value_stats.mean:sc.final_value_multiplier;
const finalVal=Math.round(pv*finalMult);
const pnl=finalVal-pv;
const pnlColor=pnl>=0?'var(--gn)':'var(--rd)';
const usdRate=18.5; // approximate USDZAR rate
const pvUSD=Math.round(pv/usdRate);
const finalUSD=Math.round(finalVal/usdRate);
const pnlUSD=finalUSD-pvUSD;
container.appendChild(el('div',{cl:'cd mb fi',st:{padding:'10px',borderLeft:'3px solid '+pnlColor}},
el('div',{st:{display:'flex',alignItems:'baseline',gap:'8px',flexWrap:'wrap'}},
el('span',{st:{fontSize:'10px',color:'var(--t3)'}},'Final Portfolio Value: '),
el('span',{st:{fontFamily:'JetBrains Mono',fontSize:'16px',fontWeight:'700',color:pnlColor}},finalVal.toLocaleString()+' ZAR'),
el('span',{st:{fontSize:'9px',color:pnlColor}},'('+(pnl>=0?'+':'')+pnl.toLocaleString()+' ZAR)'),
el('span',{st:{fontSize:'10px',color:'var(--t3)',marginLeft:'8px'}},'|'),
el('span',{st:{fontFamily:'JetBrains Mono',fontSize:'13px',fontWeight:'600',color:'var(--t2)',marginLeft:'4px'}},'$'+finalUSD.toLocaleString()+' USD'),
el('span',{st:{fontSize:'9px',color:pnlColor}},'('+(pnlUSD>=0?'+$':'-$')+Math.abs(pnlUSD).toLocaleString()+')')
),
el('div',{st:{fontSize:'8px',color:'var(--t3)',marginTop:'3px'}},'Initial: '+pv.toLocaleString()+' ZAR ($'+pvUSD.toLocaleString()+' USD)'),
isMC?el('div',{st:{fontSize:'8px',color:'var(--t3)',marginTop:'2px'}},'5th pctile: '+Math.round(pv*sc.final_value_stats.p5).toLocaleString()+' ZAR | 95th pctile: '+Math.round(pv*sc.final_value_stats.p95).toLocaleString()+' ZAR'):null
));

// Path selector (MC only) + View mode
if(isMC){
container.appendChild(el('div',{st:{display:'flex',gap:'8px',marginBottom:'10px',alignItems:'center'}},
el('label',{cl:'fl',st:{margin:0}},'Path:'),
...['Representative','Best','Worst'].map(p=>el('button',{cl:'tbtn'+(SIM.currentPath===p?' tbtn-a':''),st:{fontSize:'8px'},onClick:function(){SIM.currentPath=p;renderSimResults();}},p)),
el('span',{st:{margin:'0 8px',color:'var(--bd)'}},'|'),
el('label',{cl:'fl',st:{margin:0}},'View:'),
...['Detailed View','Compare Scenarios'].map(v=>el('button',{cl:'tbtn'+(SIM.viewMode===v?' tbtn-a':''),st:{fontSize:'8px'},onClick:function(){SIM.viewMode=v;renderSimResults();}},v))
));
}

// Holdings Pie Chart
const simPCR2=isMC?(sc.paths[SIM.currentPath.toLowerCase()]||sc.paths.representative).pcr:sc.pcr;
if(simPCR2&&Object.keys(simPCR2).length){
const pieColors2=['#1F4E79','#2E75B6','#5896f0','#C9A227','#E67E22','#2dd4a0','#9878e8','#f06060','#20c8e0','#d4973a'];
container.appendChild(el('div',{cl:'cd mb fi'},
el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Holdings Allocation & Risk Contribution')),
el('div',{cl:'g2',st:{gap:'10px'}},
el('div',{st:{height:'180px'}},el('canvas',{id:'sim-pie'})),
el('div',{st:{height:'180px'}},el('canvas',{id:'sim-pie2'})))));
setTimeout(()=>{
const tks=Object.keys(simPCR2);
const cv1=document.getElementById('sim-pie');if(cv1)CTS['sim-pie']=new Chart(cv1,{type:'doughnut',data:{labels:tks,datasets:[{data:tks.map(t=>Math.abs(simPCR2[t]||0)),backgroundColor:tks.map((_,i)=>pieColors2[i%pieColors2.length]),borderWidth:0}]},options:{responsive:true,maintainAspectRatio:false,cutout:'60%',plugins:{legend:{position:'right',labels:{color:'#8d9db8',font:{size:8},boxWidth:7}},title:{display:true,text:'Risk Contribution (%)',color:'#586880',font:{size:9}}}}});
const blPCR2=sc.baseline_pcr||{};
const cv2=document.getElementById('sim-pie2');if(cv2)CTS['sim-pie2']=new Chart(cv2,{type:'doughnut',data:{labels:tks,datasets:[{data:tks.map(t=>Math.abs(blPCR2[t]||simPCR2[t]||0)),backgroundColor:tks.map((_,i)=>pieColors2[i%pieColors2.length]+'88'),borderWidth:0}]},options:{responsive:true,maintainAspectRatio:false,cutout:'60%',plugins:{legend:{position:'right',labels:{color:'#8d9db8',font:{size:8},boxWidth:7}},title:{display:true,text:'Baseline Allocation (%)',color:'#586880',font:{size:9}}}}});
},80);
}

// Charts row: Equity Curve + PCR
container.appendChild(el('div',{cl:'g2 mb',st:{gap:'10px'}},
el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Equity Curve')),el('div',{cl:'cc cc-t'},el('canvas',{id:'sim-eq'}))),
el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Percent Contribution of Risk')),el('div',{cl:'cc cc-t'},el('canvas',{id:'sim-pcr'})))
));

// Render charts
setTimeout(()=>{
// Equity curve
const eqCv=document.getElementById('sim-eq');
if(eqCv){
const datasets=[];
if(isMC&&SIM.viewMode==='Detailed View'){
const pathNames=['Representative','Best','Worst'];
const pathColors=['#5896f0','#2dd4a0','#f06060'];
const paths=[sc.all_cumulative[0],sc.all_cumulative[1],sc.all_cumulative[2]];
pathNames.forEach((pn,i)=>{
const isActive=SIM.currentPath===pn;
datasets.push({label:pn,data:paths[i].map(v=>Math.round(pv*v)),borderColor:pathColors[i],backgroundColor:'transparent',borderWidth:isActive?3:1.5,pointRadius:0,tension:.3,borderDash:isActive?[]:[4,2]});
});
}else{
// Compare across scenarios
const colors=['#5896f0','#2dd4a0','#f06060','#d4973a','#9878e8','#20c8e0','#e8ad50','#f09838'];
let ci=0;
Object.entries(SIM.scenarios).forEach(([sn,sd])=>{
const cum=sd.type==='Monte Carlo'?sd.all_cumulative[0]:sd.cumulative_returns;
const isActive=sn===SIM.currentScenario;
datasets.push({label:sn,data:cum.map(v=>Math.round(pv*v)),borderColor:colors[ci%colors.length],backgroundColor:'transparent',borderWidth:isActive?3:1.5,pointRadius:0,tension:.3});
ci++;
});
}
const maxLen=Math.max(...datasets.map(d=>d.data.length));
const labels=Array.from({length:maxLen},(_,i)=>i+1);
CTS['sim-eq']=new Chart(eqCv,{type:'line',data:{labels,datasets},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:'#586880',font:{size:8},boxWidth:8}},tooltip:{mode:'index',intersect:false,backgroundColor:'#1e2a3c',titleColor:'#e8ecf4',bodyColor:'#8d9db8',titleFont:{size:9},bodyFont:{size:9}}},scales:{x:{title:{display:true,text:'Days',color:'#586880',font:{size:8}},grid:{color:'rgba(37,48,69,.2)'},ticks:{color:'#586880',font:{size:7},maxTicksLimit:8},border:{display:false}},y:{title:{display:true,text:'Portfolio Value (ZAR)',color:'#586880',font:{size:8}},grid:{color:'rgba(37,48,69,.2)'},ticks:{color:'#586880',font:{size:7},callback:v=>(v/1000).toFixed(0)+'k'},border:{display:false}}}}});
}

// PCR chart
const pcrCv=document.getElementById('sim-pcr');
if(pcrCv){
const simPCR=isMC?(sc.paths[SIM.currentPath.toLowerCase()]||sc.paths.representative).pcr:sc.pcr;
const blPCR=sc.baseline_pcr||{};
const tickers=Object.keys(simPCR);
CTS['sim-pcr']=new Chart(pcrCv,{type:'bar',data:{labels:tickers,datasets:[{label:'Simulated',data:tickers.map(t=>simPCR[t]||0),backgroundColor:'rgba(8,48,107,.7)',borderWidth:0,borderRadius:2},{label:'Baseline',data:tickers.map(t=>blPCR[t]||0),backgroundColor:'rgba(107,174,214,.5)',borderWidth:0,borderRadius:2}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:'#586880',font:{size:8},boxWidth:8}}},scales:{x:{title:{display:true,text:'Assets',color:'#586880',font:{size:8}},grid:{display:false},ticks:{color:'#586880',font:{size:7}},border:{display:false}},y:{title:{display:true,text:'PCR (%)',color:'#586880',font:{size:8}},grid:{color:'rgba(37,48,69,.2)'},ticks:{color:'#586880',font:{size:7}},border:{display:false}}}}});
}
},80);

// Factor Analysis (if available)
if(isMC&&sc.factor_analysis&&sc.factor_analysis.classifications){
const fa=sc.factor_analysis;
const tickers=Object.keys(fa.classifications);
container.appendChild(el('div',{cl:'cd fi',st:{marginTop:'10px'}},
el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Factor Classification & Stress')),
el('table',{cl:'dt'},
el('thead',null,el('tr',null,el('th',null,'Asset'),...fa.factors_used.map(f=>el('th',null,f)),el('th',null,'R²'))),
el('tbody',null,...tickers.map(t=>el('tr',null,
el('td',{st:{fontWeight:'500',color:'var(--t1)'}},t),
...fa.factors_used.map(f=>{const cls=fa.classifications[t][f]||'—';const beta=fa.regression[t].betas[f];return el('td',{st:{fontSize:'8px'}},el('div',{st:{color:'var(--t1)'}},cls),el('div',{cl:'mn',st:{fontSize:'7px',color:'var(--t3)'}},beta!=null?beta.toFixed(3):''));}),
el('td',{cl:'mn',st:{color:'var(--ac)'}},fa.regression[t].r_squared!=null?fa.regression[t].r_squared.toFixed(3):'—')
)))
)));
}

// HMM regime info (MC only)
if(isMC&&sc.vol_regimes){
container.appendChild(el('div',{cl:'cd fi',st:{marginTop:'10px'}},
el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'HMM Volatility Regimes'),el('span',{st:{fontSize:'8px',color:sc.hmm_available?'var(--gn)':'var(--or)'}},sc.hmm_available?'hmmlearn active':'Fallback mode')),
el('div',{cl:'g3',st:{gap:'8px'}},
...Object.entries(sc.vol_regimes).map(([r,v])=>el('div',{st:{background:r===sc.regime?'rgba(212,151,58,.08)':'var(--b0)',border:'1px solid '+(r===sc.regime?'var(--ac)':'var(--bd)'),borderRadius:'5px',padding:'8px'}},
el('div',{st:{fontSize:'8px',color:'var(--t3)',textTransform:'uppercase'}},r+(r===sc.regime?' (Active)':'')),
el('div',{cl:'mn',st:{fontSize:'14px',fontWeight:'600',color:r===sc.regime?'var(--ac)':'var(--t1)'}},(v*100).toFixed(2)+'%')
))
)));
}

// KDE Distribution Charts (across all scenarios)
if(Object.keys(SIM.scenarios).length>=1){
const kdeMetrics=['annual_volatility','expected_return','sharpe_ratio','var_95','max_drawdown'];
const kdeLabels={'annual_volatility':'Annual Volatility','expected_return':'Expected Return','sharpe_ratio':'Sharpe Ratio','var_95':'95% VaR','max_drawdown':'Max Drawdown'};
container.appendChild(el('div',{cl:'cd fi',st:{marginTop:'10px'}},
el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'KDE Comparison of Portfolio Metrics Across Simulations')),
el('div',{st:{marginBottom:'8px'}},
el('label',{cl:'fl'},'Choose Metric KDE to Display:'),
el('select',{cl:'fin',id:'kde-metric',onChange:function(){renderKDE(this.value);}},
...kdeMetrics.map(m=>el('option',{value:m},kdeLabels[m])))),
el('div',{cl:'cc cc-t'},el('canvas',{id:'kde-chart'}))
));
setTimeout(()=>renderKDE('annual_volatility'),100);
}
}

async function renderKDE(metric){
if(!PID)return;
try{
const r=await api('/api/v1/simulator/'+PID+'/kde',{method:'POST',body:JSON.stringify({metric})});
if(r.error)return;
const cv=document.getElementById('kde-chart');if(!cv)return;
if(CTS['kde-chart'])CTS['kde-chart'].destroy();
const colors=['#5896f0','#2dd4a0','#f06060','#d4973a','#9878e8','#20c8e0'];
const datasets=[];let ci=0;
const metricLabels={'annual_volatility':'Annual Volatility','expected_return':'Expected Return','sharpe_ratio':'Sharpe Ratio','var_95':'95% VaR','max_drawdown':'Max Drawdown'};
Object.entries(r.curves||{}).forEach(([name,data])=>{
if(data.type==='curve'){
datasets.push({label:name,data:data.x.map((x,i)=>({x,y:data.y[i]})),borderColor:colors[ci%colors.length],backgroundColor:'transparent',borderWidth:2,pointRadius:0,tension:.4,showLine:true});
}else{
datasets.push({label:name+' (point)',data:[{x:data.x,y:0}],borderColor:colors[ci%colors.length],backgroundColor:colors[ci%colors.length],pointRadius:6,showLine:false});
}
ci++;
});
CTS['kde-chart']=new Chart(cv,{type:'scatter',data:{datasets},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:'#586880',font:{size:8},boxWidth:8}},title:{display:true,text:'KDE Distribution of '+(metricLabels[metric]||metric)+' Across All Scenarios',color:'#e8ecf4',font:{size:12}}},scales:{x:{title:{display:true,text:metricLabels[metric]||metric,color:'#586880',font:{size:9}},grid:{color:'rgba(37,48,69,.2)'},ticks:{color:'#586880',font:{size:7}},border:{display:false}},y:{title:{display:true,text:'Density',color:'#586880',font:{size:9}},grid:{color:'rgba(37,48,69,.2)'},ticks:{color:'#586880',font:{size:7}},border:{display:false}}}}});
}catch(e){console.error('KDE error:',e);}
}

// === INTELLIGENCE ===
function vIntel(ct){
const it=DT.intelligence||{},rec=it.recommendation||{};
ct.appendChild(el('div',{cl:'cd mb fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'InteliRisk v4 Intelligence Report'),el('span',{cl:'cd-b',st:{background:it.color||'var(--ac)',color:'var(--b0)'}},it.level||'')),el('div',{st:{fontSize:'12px',fontWeight:'600',lineHeight:'1.6',marginBottom:'10px'}},it.headline||''),
el('div',{cl:'g5',st:{gap:'6px'}},
el('div',{st:{background:'var(--b0)',borderRadius:'5px',padding:'8px'}},el('div',{st:{fontSize:'7px',color:'var(--t3)',textTransform:'uppercase'}},'REGIME'),el('div',{st:{fontSize:'13px',fontWeight:'600',color:RCLR[it.active_regime]||'var(--t1)'}},it.active_regime||'—')),
el('div',{st:{background:'var(--b0)',borderRadius:'5px',padding:'8px'}},el('div',{st:{fontSize:'7px',color:'var(--t3)',textTransform:'uppercase'}},'CRISIS PROB'),el('div',{st:{fontSize:'13px',fontWeight:'600',color:it.crisis_probability>0.3?'var(--rd)':'var(--gn)'}},((it.crisis_probability||0)*100).toFixed(0)+'%')),
el('div',{st:{background:'var(--b0)',borderRadius:'5px',padding:'8px'}},el('div',{st:{fontSize:'7px',color:'var(--t3)',textTransform:'uppercase'}},'HERDING'),el('div',{st:{fontSize:'13px',fontWeight:'600',color:it.herding?.detected?'var(--rd)':'var(--gn)'}},it.herding?.level||'None')),
el('div',{st:{background:'var(--b0)',borderRadius:'5px',padding:'8px'}},el('div',{st:{fontSize:'7px',color:'var(--t3)',textTransform:'uppercase'}},'SHOCK TYPE'),el('div',{st:{fontSize:'13px',fontWeight:'600',color:'var(--or)'}},it.shock_classification||'—')),
el('div',{st:{background:'rgba(212,151,58,.08)',borderRadius:'5px',padding:'8px',border:'1px solid rgba(212,151,58,.1)'}},el('div',{st:{fontSize:'7px',color:'var(--ac)',textTransform:'uppercase'}},'POSTURE'),el('div',{st:{fontSize:'13px',fontWeight:'600',color:'var(--ac)'}},rec.posture||'—')))));
ct.appendChild(el('div',{cl:'g2'},
el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Alerts')),el('div',null,...(it.alerts||[]).map(a=>el('div',{cl:'al al-'+(a.severity==='HIGH'?'h':'m')},el('div',{cl:'al-sv',st:{color:a.severity==='HIGH'?'var(--rd)':'var(--or)'}},a.severity+' — '+a.type),el('div',{st:{color:'var(--t2)',fontSize:'10px'}},a.msg))))),
el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Actions')),el('div',null,...(rec.actions||[]).map((a,i)=>el('div',{st:{display:'flex',gap:'7px',marginBottom:'7px',padding:'7px 9px',background:'var(--b0)',borderRadius:'4px'}},el('span',{st:{color:'var(--ac)',fontWeight:'700',fontFamily:'JetBrains Mono',fontSize:'9px'}},'0'+(i+1)),el('span',{st:{fontSize:'10px',color:'var(--t2)',lineHeight:'1.4'}},a)))))));}

// === HOLDINGS ===
async function vHoldings(ct){
const ms=(DT.risk_core||{}).market_summary||{};
// Market-level risk summary (existing)
ct.appendChild(el('div',{cl:'cd mb fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Market Portfolio Risk — '+Object.keys(ms).length+' markets')),
el('table',{cl:'dt'},el('thead',null,el('tr',null,...['Market','Vol 21d','Vol 63d','HAR Forecast','Max DD','VaR 5%','CVaR','Comp VaR%','Skew','Kurt'].map(t=>el('th',null,t)))),
el('tbody',null,...Object.entries(ms).map(([m,d])=>el('tr',null,el('td',{st:{fontWeight:'500',color:({JSE_SA:'#1F4E79'}[m])||'var(--t1)'}},d.name||m),el('td',{cl:'mn'},fmtPct(d.vol_21d)),el('td',{cl:'mn'},fmtPct(d.vol_63d)),el('td',{cl:'mn',st:{color:'var(--pu)'}},fmtPct(d.har_forecast)),el('td',{cl:'mn',st:{color:'var(--or)'}},fmtPct(d.max_dd)),el('td',{cl:'mn',st:{color:'var(--rd)'}},fmt(d.current_var,3)),el('td',{cl:'mn',st:{color:'var(--rd)'}},fmt(d.current_cvar,3)),el('td',{cl:'mn',st:{color:'var(--ac)'}},fmtPct(d.component_var_pct)),el('td',{cl:'mn'},fmt(d.skewness,3)),el('td',{cl:'mn'},fmt(d.kurtosis,3))))))));

// Individual Stock Holdings
const holdingsWrap=el('div',{cl:'cd mb fi'});
ct.appendChild(holdingsWrap);
holdingsWrap.appendChild(el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Individual Stock Holdings (JSE Equities)'),el('div',{cl:'sp',st:{width:'14px',height:'14px'},id:'hold-sp'})));

try{
const hd=await api('/api/v1/portfolios/'+PID+'/holdings');
const h=hd.holdings||[];
const spn=$('#hold-sp');if(spn)spn.remove();
if(h.length===0){
holdingsWrap.appendChild(el('div',{st:{fontSize:'10px',color:'var(--t3)',padding:'12px',textAlign:'center'}},'No holdings found. Upload a portfolio or import from TWS.'));
}else{
// Summary cards
const totalVal=h.reduce((s,x)=>s+Math.abs(x.market_value||0),0);
const usdRate=18.5;
holdingsWrap.appendChild(el('div',{cl:'g4 mb',st:{gap:'8px'}},
kpi('Holdings',h.length,'','var(--ac)','Total positions'),
kpi('Total Value',Math.round(totalVal).toLocaleString(),'ZAR','var(--gn)','$'+Math.round(totalVal/usdRate).toLocaleString()+' USD'),
kpi('Markets',[...new Set(h.map(x=>x.market).filter(Boolean))].length,'','var(--bl)','Exchanges'),
kpi('Sectors',[...new Set(h.map(x=>x.sector).filter(x=>x&&x!=='Unknown'))].length,'','var(--pu)','Unique')
));

// Allocation chart + sector breakdown
const pieId2='hld-pie2-'+Date.now();
const sectorMap={};
h.forEach(x=>{const s=x.sector||'Unknown';sectorMap[s]=(sectorMap[s]||0)+Math.abs(x.market_value||0);});
holdingsWrap.appendChild(el('div',{cl:'g2 mb',st:{gap:'10px'}},
el('div',{st:{height:'220px'}},el('canvas',{id:pieId2})),
el('div',null,el('div',{cl:'cd-t',st:{marginBottom:'6px'}},'Sector Breakdown'),
...Object.entries(sectorMap).sort((a,b)=>b[1]-a[1]).map(([sec,val])=>
el('div',{st:{display:'flex',justifyContent:'space-between',padding:'4px 0',borderBottom:'1px solid var(--bd)',fontSize:'9px'}},
el('span',{st:{color:'var(--t1)'}},sec),
el('span',{cl:'mn',st:{color:'var(--t2)'}},Math.round(val).toLocaleString()+' ZAR ('+((val/totalVal)*100).toFixed(1)+'%)')
)))
));
setTimeout(()=>{
const colors2=['#1F4E79','#2E75B6','#5896f0','#C9A227','#E67E22','#2dd4a0','#9878e8','#f06060','#20c8e0','#d4973a'];
mkDoughnut(pieId2,h.map(x=>x.asset_id),h.map(x=>Math.abs(x.market_value||0)),h.map((_,i)=>colors2[i%colors2.length]));
},60);

// Full holdings table
holdingsWrap.appendChild(el('table',{cl:'dt'},
el('thead',null,el('tr',null,...['Symbol','Market','Sector','Qty','Avg Price','Mkt Value','Weight','Currency'].map(t=>el('th',null,t)))),
el('tbody',null,...h.sort((a,b)=>Math.abs(b.market_value||0)-Math.abs(a.market_value||0)).map(x=>{
const w=totalVal>0?Math.abs(x.market_value||0)/totalVal:0;
return el('tr',null,
el('td',{st:{fontWeight:'600',color:'var(--t1)'}},x.asset_id),
el('td',null,MARKETS[x.market]?.name||x.market||'—'),
el('td',{st:{color:'var(--t3)'}},x.sector||'—'),
el('td',{cl:'mn'},String(x.quantity||0)),
el('td',{cl:'mn'},fmt(x.price,2)),
el('td',{cl:'mn',st:{color:'var(--ac)',fontWeight:'500'}},Math.round(x.market_value||0).toLocaleString()),
el('td',{cl:'mn'},((w)*100).toFixed(1)+'%'),
el('td',null,x.currency||'ZAR'));}))
));
}
}catch(e){
const spn=$('#hold-sp');if(spn)spn.remove();
holdingsWrap.appendChild(el('div',{st:{fontSize:'10px',color:'var(--rd)',padding:'12px'}},'Could not load holdings: '+e.message));
}
}

// === BLOOMBERG LIVE ===
async function vTrades(ct){
// Stop any previous live-refresh timer
if(_liveTimer){clearInterval(_liveTimer);_liveTimer=null;}

// ── Status + connection cards ──────────────────────────────────────────────
const statusRow=el('div',{cl:'g3 mb'});ct.appendChild(statusRow);
const connCard=el('div',{cl:'cd fi'});
const blpCard =el('div',{cl:'cd fi'});
const srcCard  =el('div',{cl:'cd fi'});
statusRow.appendChild(connCard);statusRow.appendChild(blpCard);statusRow.appendChild(srcCard);

// ── Live price grid ────────────────────────────────────────────────────────
const liveWrap=el('div',{cl:'cd mb fi'});ct.appendChild(liveWrap);
liveWrap.appendChild(el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Live Market Prices — Bloomberg BLPAPI'),
  el('span',{id:'blp-ts',st:{fontSize:'8px',color:'var(--t3)'}},'fetching...')));
const liveGrid=el('div',{id:'live-grid',cl:'g4',st:{gap:'8px'}});liveWrap.appendChild(liveGrid);

// ── Portfolio Holdings ─────────────────────────────────────────────────────
const holdWrap=el('div',{cl:'cd fi'});ct.appendChild(holdWrap);
holdWrap.appendChild(el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Portfolio Holdings (from TWS)')));
const holdBody=el('div',{id:'hold-body'});holdWrap.appendChild(holdBody);

// Helpers
const MKT_NAMES={JSE_SA:'JSE (South Africa)'};
const MKT_COLORS={JSE_SA:'#1F4E79'};
const MACRO_LABELS={vix:'VIX',dxy:'DXY',oil:'Brent Oil',gold:'Gold',yield_SA:'SA 10Y Yield',yield_MA:'MA 10Y Yield',yield_NG:'NG 10Y Yield',USDZAR:'USD/ZAR',USDMAD:'USD/MAD',USDNGN:'USD/NGN'};

function priceTile(label,q,color){
  const px=q.price!=null?fmt(q.price,2):'—';
  const ch=q.change_pct!=null?q.change_pct:null;
  const chStr=ch!=null?(ch>=0?'+':'')+fmt(ch,2)+'%':'—';
  const chClr=ch==null?'var(--t3)':ch>=0?'var(--gn)':'var(--rd)';
  const dot=el('div',{st:{width:'6px',height:'6px',borderRadius:'50%',background:q.source==='bloomberg_live'?'var(--gn)':'var(--or)',marginBottom:'4px'}});
  return el('div',{st:{background:'var(--b0)',borderRadius:'6px',padding:'10px',border:'1px solid var(--bd)'}},
    dot,
    el('div',{st:{fontSize:'8px',color:color||'var(--t3)',textTransform:'uppercase',letterSpacing:'.4px',marginBottom:'3px'}},label),
    el('div',{st:{fontFamily:'JetBrains Mono',fontSize:'18px',fontWeight:'600',color:'var(--t1)',marginBottom:'2px'}},px),
    el('div',{st:{fontFamily:'JetBrains Mono',fontSize:'11px',color:chClr}},chStr),
    el('div',{st:{fontSize:'7px',color:'var(--t3)',marginTop:'3px'}},q.ticker||''));
}

async function fetchAndRender(){
  try{
    const st=await api('/api/v1/bloomberg/status');
    const isConn=st.connected===true;
    const hasBLP=st.blpapi_available===true;

    // Connection card
    connCard.innerHTML='';
    connCard.appendChild(el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Bloomberg Terminal')));
    connCard.appendChild(el('div',{st:{fontSize:'16px',fontWeight:'600',color:isConn?'var(--gn)':'var(--rd)',marginBottom:'4px'}},isConn?'Connected':'Disconnected'));
    connCard.appendChild(el('div',{st:{fontSize:'10px',color:'var(--t2)',marginBottom:'3px'}},hasBLP?'blpapi installed':'blpapi not installed'));
    connCard.appendChild(el('div',{st:{fontSize:'9px',color:'var(--t3)'}},st.host+':'+st.port));
    if(st.subscription_alive)connCard.appendChild(el('div',{st:{fontSize:'8px',color:'var(--gn)',marginTop:'4px'}},'◉ Live subscription active ('+st.live_tickers+' tickers)'));

    // Bloomberg card
    blpCard.innerHTML='';
    blpCard.appendChild(el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Market Coverage')));
    const mkts=Object.entries(st.market_tickers||{});
    mkts.forEach(([id,ticker])=>{
      blpCard.appendChild(el('div',{cl:'fb',st:{marginBottom:'4px',fontSize:'10px'}},
        el('span',{st:{color:MKT_COLORS[id]||'var(--t1)',fontWeight:'500'}},MKT_NAMES[id]||id),
        el('span',{st:{fontFamily:'JetBrains Mono',fontSize:'8px',color:'var(--t3)'}},ticker)));
    });

    // Source card
    srcCard.innerHTML='';
    srcCard.appendChild(el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Data Source')));
    const src=st.data_source||'unknown';
    const srcClr=src==='bloomberg'?'var(--gn)':src==='tws'?'var(--gn)':src==='csv'?'var(--ac)':'var(--or)';
    srcCard.appendChild(el('div',{st:{fontSize:'15px',fontWeight:'600',color:srcClr,marginBottom:'4px',textTransform:'uppercase'}},src==='tws'?'TWS (IBKR)':src));
    srcCard.appendChild(el('div',{st:{fontSize:'10px',color:'var(--t2)',lineHeight:'1.5'}},
      src==='bloomberg'?'Live Bloomberg BLPAPI feed → InteliRisk v4 pipeline':
      src==='tws'?'IBKR TWS historical data + CSV → InteliRisk v4 pipeline':
      src==='csv'?'Historical CSV files (data/intellirisk/) → InteliRisk v4 pipeline':
      'Synthetic regime-switching GBM → InteliRisk v4 pipeline'));
    if(st.tws_connected)srcCard.appendChild(el('div',{st:{fontSize:'8px',color:'var(--gn)',marginTop:'4px'}},'◉ TWS Connected'));
    srcCard.appendChild(el('button',{cl:'tbtn',st:{marginTop:'8px',fontSize:'9px'},onClick:async()=>{
      try{await api('/api/v1/bloomberg/refresh',{method:'POST'});await recompute();}catch(e){}
    }},'⟳ Refresh Data'));

    // Live quotes — from Bloomberg or show CSV/TWS latest
    if(isConn){
      try{
        const lv=await api('/api/v1/bloomberg/live');
        const ts=lv.timestamp?lv.timestamp.slice(11,19):'';
        const tsEl=$('#blp-ts');if(tsEl)tsEl.textContent='Last update: '+ts+' UTC';
        const quotes=lv.quotes||{};
        const mkKeys=['JSE_SA'];
        liveGrid.innerHTML='';
        mkKeys.forEach(k=>{const q=quotes[k];if(q)liveGrid.appendChild(priceTile(MKT_NAMES[k]||k,q,MKT_COLORS[k]));});
        if(!liveGrid.childElementCount)liveGrid.appendChild(el('div',{st:{color:'var(--t3)',fontSize:'10px',padding:'10px'}},'Waiting for Bloomberg data...'));
      }catch(e){liveGrid.innerHTML='';liveGrid.appendChild(el('div',{st:{color:'var(--t3)',fontSize:'10px',padding:'10px'}},'Could not fetch live quotes'));}
    } else {
      liveGrid.innerHTML='';
      const msg=st.tws_connected?'Data sourced from TWS historical. Bloomberg not connected.':
        hasBLP?'Bloomberg Terminal not running — start Bloomberg and click Refresh Data.':
        'Install blpapi: pip install --index-url=https://blpapi.bloomberg.com/repository/releases/python/simple blpapi';
      liveGrid.appendChild(el('div',{st:{color:'var(--t3)',fontSize:'10px',padding:'10px',gridColumn:'1/-1'}},msg));
    }
  }catch(e){
    connCard.innerHTML='<div class="cl2" style="color:var(--rd)">Status error: '+e.message+'</div>';
  }

  // Portfolio Holdings (real data, not fake trades)
  try{
    const td=await api('/api/v1/bank/trades');
    holdBody.innerHTML='';
    const hlds=td.holdings||[];
    if(hlds.length===0){
      holdBody.appendChild(el('div',{st:{color:'var(--t3)',fontSize:'10px',padding:'16px',textAlign:'center'}},'No portfolio loaded. Go to Upload to import your portfolio.'));
    } else {
      // Pie chart for holdings allocation
      const pieId='hold-pie-'+Date.now();
      holdBody.appendChild(el('div',{cl:'g2 mb',st:{gap:'10px'}},
        el('div',{st:{height:'200px'}},el('canvas',{id:pieId})),
        el('div',null,
          el('div',{st:{fontSize:'10px',color:'var(--t3)',marginBottom:'6px'}},'Total Holdings: '+hlds.length),
          el('div',{st:{fontSize:'10px',color:'var(--t3)',marginBottom:'6px'}},'Total Value: '+Math.round(hlds.reduce((s,h)=>s+Math.abs(h.market_value||0),0)).toLocaleString()+' ZAR'),
          ...hlds.slice(0,8).map(h=>el('div',{st:{display:'flex',justifyContent:'space-between',fontSize:'9px',padding:'3px 0',borderBottom:'1px solid var(--bd)'}},
            el('span',{st:{color:'var(--t1)',fontWeight:'500'}},h.asset_id),
            el('span',{cl:'mn',st:{color:'var(--t2)'}},Math.round(h.market_value||0).toLocaleString()+' '+(h.currency||'ZAR'))))
        )
      ));
      // Render pie chart
      setTimeout(()=>{
        const cv=document.getElementById(pieId);if(!cv)return;
        const colors=['#1F4E79','#2E75B6','#5896f0','#C9A227','#E67E22','#2dd4a0','#9878e8','#f06060','#20c8e0','#d4973a','#f09838'];
        CTS[pieId]=new Chart(cv,{type:'doughnut',data:{labels:hlds.map(h=>h.asset_id),datasets:[{data:hlds.map(h=>Math.abs(h.market_value||0)),backgroundColor:hlds.map((_,i)=>colors[i%colors.length]),borderWidth:0}]},options:{responsive:true,maintainAspectRatio:false,cutout:'60%',plugins:{legend:{position:'right',labels:{color:'#8d9db8',font:{size:8},boxWidth:7,padding:4}}}}});
      },60);
      // Holdings table
      holdBody.appendChild(el('table',{cl:'dt'},
        el('thead',null,el('tr',null,...['Symbol','Market','Sector','Qty','Price','Value','Weight'].map(t=>el('th',null,t)))),
        el('tbody',null,...hlds.map(h=>el('tr',null,
          el('td',{st:{fontWeight:'500',color:'var(--t1)'}},h.asset_id),
          el('td',null,MARKETS[h.market]?.name||h.market||'—'),
          el('td',{st:{color:'var(--t3)'}},h.sector||'—'),
          el('td',{cl:'mn'},String(h.quantity||0)),
          el('td',{cl:'mn'},fmt(h.price,2)+' '+(h.currency||'ZAR')),
          el('td',{cl:'mn',st:{color:'var(--ac)'}},Math.round(h.market_value||0).toLocaleString()),
          el('td',{cl:'mn'},((h.weight||0)*100).toFixed(1)+'%'))))));
    }
  }catch(e){holdBody.innerHTML='<div style="color:var(--t3);font-size:10px;padding:10px">Could not load holdings</div>';}
}

fetchAndRender();
// Auto-refresh live quotes every 10 seconds while on this view
_liveTimer=setInterval(()=>{if(VW==='trades')fetchAndRender().catch(()=>{});else{clearInterval(_liveTimer);_liveTimer=null;}},10000);
}

// === UPLOAD ===
function vUpload(ct){
// Top: IBKR direct import
ct.appendChild(el('div',{cl:'cd mb fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Connect IBKR TWS (Direct Import)'),el('span',{id:'ibkr-st',cl:'cd-b',st:{background:'var(--b3)',color:'var(--t3)'}},'Checking...')),
el('div',{cl:'g4',st:{gap:'8px'}},
el('div',{st:{display:'flex',gap:'6px',alignItems:'center'}},el('label',{st:{fontSize:'9px',color:'var(--t3)',width:'30px'}},'Host'),el('input',{cl:'fin',id:'ibkr-host',value:'127.0.0.1',st:{width:'100px',padding:'5px 7px',fontSize:'10px'}})),
el('div',{st:{display:'flex',gap:'6px',alignItems:'center'}},el('label',{st:{fontSize:'9px',color:'var(--t3)',width:'30px'}},'Port'),el('input',{cl:'fin',id:'ibkr-port',value:'7497',st:{width:'70px',padding:'5px 7px',fontSize:'10px'}})),
el('button',{cl:'tbtn',st:{fontSize:'10px'},onClick:async()=>{
const st=$('#ibkr-st');st.textContent='Connecting...';st.style.background='var(--or)';
try{const r=await api('/api/v1/ibkr/connect',{method:'POST',body:JSON.stringify({host:$('#ibkr-host').value,port:$('#ibkr-port').value})});
st.textContent=r.connected?'Connected':'Failed';st.style.background=r.connected?'var(--gn)':'var(--rd)';st.style.color=r.connected?'#fff':'#fff';
}catch(e){st.textContent='Error';st.style.background='var(--rd)';}
}},'Connect'),
el('button',{cl:'tbtn tbtn-a',st:{fontSize:'10px'},onClick:async()=>{
try{const r=await api('/api/v1/ibkr/import-portfolio',{method:'POST',body:JSON.stringify({name:'IBKR Portfolio'})});
PID=r.portfolio_id;showLoading();DT=await api('/api/v1/portfolios/'+PID+'/compute-all');buildLayout();VW='overview';renderView();
}catch(e){alert('IBKR import failed: '+e.message);}
}},'Import Portfolio from TWS')),
el('div',{st:{fontSize:'9px',color:'var(--t3)',marginTop:'6px'}},'TWS Workstation must be running. Paper: 7497, Live: 7496, Gateway: 4001/4002')));
// Check IBKR status
api('/api/v1/ibkr/status').then(r=>{const st=$('#ibkr-st');if(st){st.textContent=r.connected?'Connected':'Disconnected';st.style.background=r.connected?'var(--gn)':'var(--b3)';st.style.color=r.connected?'#fff':'var(--t3)';}}).catch(()=>{});

ct.appendChild(el('div',{cl:'g2'},
// Left: Upload area
el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Upload Portfolio (CSV / Excel)')),
el('div',{st:{fontSize:'11px',color:'var(--t2)',lineHeight:'1.6',marginBottom:'14px'}},'Export your portfolio from Bloomberg PORT, IBKR TWS, your broker, or Excel. Supported: CSV, XLSX, XLS.'),
el('div',{st:{fontSize:'10px',color:'var(--t3)',marginBottom:'10px'}},'Required column: symbol/ticker. Optional: market/exchange, quantity, price, sector, currency.'),
el('div',{cl:'fg'},el('label',{cl:'fl'},'Portfolio Name'),el('input',{cl:'fin',id:'up-name',value:'My Portfolio',placeholder:'Portfolio name'})),
el('div',{id:'drop-zone',st:{padding:'30px',border:'2px dashed var(--bd)',borderRadius:'8px',textAlign:'center',color:'var(--t3)',fontSize:'11px',cursor:'pointer',transition:'all .2s',marginBottom:'12px'},
onClick:()=>document.getElementById('up-file').click(),
onDragover:function(e){e.preventDefault();this.style.borderColor='var(--ac)';this.style.color='var(--ac)';},
onDragleave:function(e){this.style.borderColor='var(--bd)';this.style.color='var(--t3)';},
onDrop:function(e){e.preventDefault();this.style.borderColor='var(--bd)';const f=e.dataTransfer.files[0];if(f)doUpload(f);}},
el('div',{st:{fontSize:'20px',marginBottom:'6px'}},'⊕'),
'Drop CSV/Excel here or click to browse',el('br'),el('span',{st:{fontSize:'9px',color:'var(--t3)'}},'Bloomberg PORT, IBKR TWS export, broker CSV, or custom spreadsheet')),
el('input',{type:'file',id:'up-file',accept:'.csv,.xlsx,.xls,.tsv',st:{display:'none'},onChange:function(){if(this.files[0])doUpload(this.files[0]);}}),
el('div',{id:'up-status',st:{fontSize:'10px',color:'var(--t3)',minHeight:'20px'}}),
el('div',{st:{marginTop:'10px',display:'flex',gap:'8px'}},
el('button',{cl:'btn',st:{width:'auto',padding:'8px 16px',fontSize:'11px',background:'var(--b3)',color:'var(--t2)',border:'1px solid var(--bd)'},onClick:async()=>{
try{const r=await api('/api/v1/portfolios/create-sample',{method:'POST'});PID=r.portfolio_id;showLoading();DT=await api('/api/v1/portfolios/'+PID+'/compute-all');buildLayout();VW='overview';renderView();}catch(e){alert('Error: '+e.message);}
}},'Use Sample Portfolio'),
el('button',{cl:'btn',st:{width:'auto',padding:'8px 16px',fontSize:'11px',background:'var(--b3)',color:'var(--ac)',border:'1px solid var(--ac)'},onClick:()=>{
const csv='symbol,market,quantity,price,currency,sector\nNPN,JSE,100,3200,ZAR,Technology\nSBK,JSE,500,180,ZAR,Banking\nAGL,JSE,200,850,ZAR,Mining\nBHP,JSE,50,4500,ZAR,Mining\nFSR,JSE,300,150,ZAR,Banking\nMTN,JSE,150,1200,ZAR,Telecom\nSOL,JSE,100,750,ZAR,Energy\nAMS,JSE,80,2100,ZAR,Mining\nIMP,JSE,200,400,ZAR,Mining\nUSDZAR,FX,10000,18.5,USD,Currency';
const b=new Blob([csv],{type:'text/csv'});const a=document.createElement('a');a.href=URL.createObjectURL(b);a.download='example_portfolio.csv';a.click();
}},'Download Example CSV'))),
// Right: Format guide + example
el('div',{cl:'cd fi'},el('div',{cl:'cd-h'},el('span',{cl:'cd-t'},'Supported Formats & Example')),
el('div',{st:{fontSize:'10px',color:'var(--t2)',lineHeight:'1.7'}},
el('div',{st:{fontWeight:'600',color:'var(--ac)',marginBottom:'6px'}},'IBKR TWS Export'),
'File → Account → Export to CSV. Or use the Connect button above for direct import.',el('br'),el('br'),
el('div',{st:{fontWeight:'600',color:'var(--ac)',marginBottom:'6px'}},'Bloomberg PORT Export'),
'Type PORT <GO> → Export → CSV. Columns: Ticker, Exchange, Shares, Last Price, Sector.',el('br'),el('br'),
el('div',{st:{fontWeight:'600',color:'var(--ac)',marginBottom:'6px'}},'Broker / Custom CSV'),
'Minimum: a column with ticker/symbol names. Add exchange (JSE), quantity, price for full analysis.',el('br'),el('br'),
el('div',{st:{fontWeight:'600',color:'var(--t3)',marginBottom:'4px'}},'Exchange Codes'),
'JSE / SJ / XJSE = Johannesburg Stock Exchange (South Africa)'),
// Example table
el('div',{st:{marginTop:'12px',background:'var(--b0)',borderRadius:'6px',padding:'10px'}},el('div',{st:{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:'6px'}},el('span',{cl:'cd-t'},'Example CSV Format'),el('span',{st:{fontSize:'8px',color:'var(--t3)',fontFamily:'JetBrains Mono'}},'example_portfolio.csv')),
el('table',{cl:'dt'},el('thead',null,el('tr',null,...['symbol','market','quantity','price','currency','sector'].map(t=>el('th',null,t)))),
el('tbody',null,
el('tr',null,el('td',{cl:'mn'},'NPN'),el('td',null,'JSE'),el('td',{cl:'mn'},'100'),el('td',{cl:'mn'},'3200'),el('td',null,'ZAR'),el('td',null,'Technology')),
el('tr',null,el('td',{cl:'mn'},'SBK'),el('td',null,'JSE'),el('td',{cl:'mn'},'500'),el('td',{cl:'mn'},'180'),el('td',null,'ZAR'),el('td',null,'Banking')),
el('tr',null,el('td',{cl:'mn'},'AGL'),el('td',null,'JSE'),el('td',{cl:'mn'},'200'),el('td',{cl:'mn'},'850'),el('td',null,'ZAR'),el('td',null,'Mining')),
el('tr',null,el('td',{cl:'mn'},'MTN'),el('td',null,'JSE'),el('td',{cl:'mn'},'150'),el('td',{cl:'mn'},'1200'),el('td',null,'ZAR'),el('td',null,'Telecom')),
el('tr',null,el('td',{cl:'mn'},'SOL'),el('td',null,'JSE'),el('td',{cl:'mn'},'100'),el('td',{cl:'mn'},'750'),el('td',null,'ZAR'),el('td',null,'Energy'))))))));
}

async function doUpload(file){
const status=document.getElementById('up-status');
if(status)status.textContent='Uploading '+file.name+'...';
status.style.color='var(--ac)';
const form=new FormData();
form.append('file',file);
form.append('name',document.getElementById('up-name')?.value||'Upload');
try{
const r=await fetch('/api/v1/portfolios/upload',{method:'POST',headers:{'Authorization':'Bearer '+TK},body:form});
const d=await r.json();
if(d.error){status.textContent='Error: '+d.error;status.style.color='var(--rd)';return;}
PID=d.portfolio_id;
status.innerHTML='<span style="color:var(--gn)">✓ '+d.holdings_count+' holdings loaded ('+Object.keys(d.markets_found||{}).join(', ')+')</span>';
// Auto compute
setTimeout(async()=>{status.textContent='Computing 8 layers...';DT=await api('/api/v1/portfolios/'+PID+'/compute-all');buildLayout();VW='overview';renderView();},500);
}catch(e){status.textContent='Upload failed: '+e.message;status.style.color='var(--rd)';}
}

showLogin();

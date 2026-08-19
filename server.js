const http=require('http');
const fs=require('fs');
const path=require('path');
const crypto=require('crypto');
const PORT=process.env.PORT||3000;
const ROOT=__dirname, DB_FILE=path.join(ROOT,'nexoworld-data.json');
const DEFAULT={users:{},worlds:[],chats:[],stats:{},bans:{},blocks:{},follows:{},friends:{},likesSeen:{}};
let db=loadDB();
const clients=new Set(), presence=new Map();

function userKey(v){return clean(v).toLowerCase()}
function passwordHash(p){return crypto.createHash('sha256').update(String(p||'')).digest('hex')}
function makeToken(username){return crypto.createHash('sha256').update(username+':'+Date.now()+':'+crypto.randomBytes(12).toString('hex')).digest('hex')}
function loadDB(){try{return {...DEFAULT,...JSON.parse(fs.readFileSync(DB_FILE,'utf8'))}}catch{return JSON.parse(JSON.stringify(DEFAULT))}}
function saveDB(){fs.writeFileSync(DB_FILE,JSON.stringify(db,null,2))}
function json(res,code,obj){const body=JSON.stringify(obj);res.writeHead(code,{'Content-Type':'application/json; charset=utf-8','Access-Control-Allow-Origin':'*','Access-Control-Allow-Headers':'Content-Type','Cache-Control':'no-store'});res.end(body)}
function readBody(req){return new Promise((resolve,reject)=>{let s='';req.on('data',c=>{s+=c;if(s.length>1e6)req.destroy()});req.on('end',()=>{try{resolve(s?JSON.parse(s):{})}catch(e){reject(e)}});req.on('error',reject)})}
function clean(v){return String(v||'').trim().slice(0,40)}
function moderate(text){let out=String(text||'');for(const r of [/\bputo\b/gi,/\bcoño\b/gi,/\bculo\b/gi,/\bpito\b/gi,/\bchocho\b/gi,/\bhijo\s+de\s+puta\b/gi,/\bmierda\b/gi])out=out.replace(r,'estoy intentando decir una palabrota,pero no pude porque me pillo la moderacion de nexo!');return out.slice(0,800)}
function banned(user){const until=db.bans[user]||0;return until>Date.now()?until:0}
function stats(id){if(!db.stats[id])db.stats[id]={players:0,likes:0};return db.stats[id]}
function onlineUsers(){const o={};for(const [id,p] of presence)o[id]=p;return o}
function wsSend(c,obj){if(c.closed)return;const body=Buffer.from(JSON.stringify(obj));let h;if(body.length<126)h=Buffer.from([0x81,body.length]);else if(body.length<65536){h=Buffer.alloc(4);h[0]=0x81;h[1]=126;h.writeUInt16BE(body.length,2)}else return;c.socket.write(Buffer.concat([h,body]))}
function broadcast(obj){for(const c of clients)wsSend(c,obj)}
function updateStats(){const byGame={};for(const p of presence.values()){const g=p.gameId;if(g)byGame[g]=(byGame[g]||0)+1}Object.entries(byGame).forEach(([id,n])=>{stats(id).players=n});for(const id of Object.keys(db.stats))if(!byGame[id])db.stats[id].players=0;broadcast({type:'stats_all',games:db.stats});broadcast({type:'presence',users:onlineUsers()})}
function acceptWebSocket(req,socket){const key=req.headers['sec-websocket-key'];const accept=crypto.createHash('sha1').update(key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+'\r\n\r\n');const c={socket,buffer:Buffer.alloc(0),closed:false,username:'Invitado',gameId:null,game:'En el menú'};clients.add(c);presence.set(c,{username:c.username,game:c.game,gameId:null});wsSend(c,{type:'presence',users:onlineUsers()});wsSend(c,{type:'stats_all',games:db.stats});
 socket.on('data',data=>{if(c.closed)return;c.buffer=Buffer.concat([c.buffer,data]);while(true){const f=parseFrame(c.buffer);if(!f)break;c.buffer=f.rest;handleWs(c,f.opcode,f.payload)}});socket.on('close',()=>{c.closed=true;clients.delete(c);presence.delete(c);updateStats()});socket.on('error',()=>{c.closed=true;clients.delete(c);presence.delete(c);updateStats()})}
function parseFrame(buf){if(buf.length<2)return null;const b1=buf[0],b2=buf[1];let len=b2&127,off=2;if(len===126){if(buf.length<4)return null;len=buf.readUInt16BE(2);off=4}else if(len===127)return null;const masked=!!(b2&128);const need=off+(masked?4:0)+len;if(buf.length<need)return null;let mask;if(masked){mask=buf.subarray(off,off+4);off+=4}const payload=Buffer.from(buf.subarray(off,off+len));if(masked)for(let i=0;i<payload.length;i++)payload[i]^=mask[i%4];return{opcode:b1&15,payload,rest:buf.subarray(need)}}
function handleWs(c,opcode,payload){if(opcode===8){try{c.socket.end()}catch{};return}if(opcode!==1)return;let m;try{m=JSON.parse(payload.toString())}catch{return}
 if(m.type==='identify'){const u=clean(m.username)||'Invitado',b=banned(u);if(b){wsSend(c,{type:'banned',until:b});return}c.username=u;presence.set(c,{username:u,game:c.game,gameId:c.gameId});updateStats();return}
 if(m.type==='presence'){c.game=clean(m.game)||'En el menú';c.gameId=String(m.gameId||'');presence.set(c,{username:c.username,game:c.game,gameId:c.gameId});updateStats();return}
 if(m.type==='game_state'){const room=String(m.gameId||c.gameId||'');for(const other of clients){if(other!==c&&other.gameId===room)wsSend(other,{type:'game_state',gameId:room,state:m.state||{}})}return}
 if(m.type==='chat'){const text=moderate(m.message?.text);if(!text)return;const msg={sender:c.username,text,time:new Date().toLocaleTimeString('es-ES',{hour:'2-digit',minute:'2-digit'})};db.chats.push(msg);db.chats=db.chats.slice(-1000);saveDB();broadcast({type:'chat',message:msg});return}
 if(m.type==='city_chat'){const text=moderate(m.text);if(!text)return;const msg={sender:c.username,text,time:new Date().toLocaleTimeString('es-ES',{hour:'2-digit',minute:'2-digit'}),room:'city'};broadcast({type:'city_chat',message:msg});return}
}
const server=http.createServer(async(req,res)=>{if(req.method==='OPTIONS'){res.writeHead(204,{'Access-Control-Allow-Origin':'*','Access-Control-Allow-Headers':'Content-Type','Access-Control-Allow-Methods':'GET,POST,OPTIONS'});return res.end()}const u=new URL(req.url,'http://'+req.headers.host);try{
 if((u.pathname==='/api/register'||u.pathname==='/api/login')&&req.method==='POST'){
   const b=await readBody(req),key=userKey(b.username),password=String(b.password||'');
   if(!key||password.length<4)return json(res,400,{error:'Usuario y contraseña válidos son obligatorios'});
   if(u.pathname==='/api/register'){
     if(db.users[key])return json(res,409,{error:'Usuario ya existe'});
     db.users[key]={username:clean(b.username),passwordHash:passwordHash(password),coins:1000,level:1,friends:[],followers:[],createdAt:Date.now()};saveDB();
     return json(res,200,{user:{...db.users[key],passwordHash:undefined},token:makeToken(key)});
   }
   const found=db.users[key];if(!found||found.passwordHash!==passwordHash(password))return json(res,401,{error:'Credenciales incorrectas'});
   return json(res,200,{user:{...found,passwordHash:undefined},token:makeToken(key)});
 }
 if(u.pathname==='/api/health')return json(res,200,{ok:true,online:clients.size});
 if(u.pathname==='/api/stats')return json(res,200,{games:db.stats});
 if(u.pathname==='/api/worlds'&&req.method==='GET')return json(res,200,{worlds:db.worlds});
 if(u.pathname==='/api/worlds'&&req.method==='POST'){const b=await readBody(req),creator=clean(b.creator)||'Anónimo';const ban=banned(creator);if(ban)return json(res,403,{error:'Banned',until:ban});const world={id:crypto.randomUUID(),name:clean(b.name)||'Mundo sin nombre',template:clean(b.template)||'Ciudad',creator,objects:Array.isArray(b.objects)?b.objects.slice(0,200):[],painted:Array.isArray(b.painted)?b.painted.slice(0,200):[],createdAt:Date.now()};db.worlds.unshift(world);db.worlds=db.worlds.slice(0,250);saveDB();return json(res,200,{worlds:db.worlds,world})}
 if(u.pathname==='/api/like'&&req.method==='POST'){const b=await readBody(req),id=String(b.gameId),actor=clean(b.username)||'guest',key=actor+':'+id,s=stats(id);if(!db.likesSeen[key]){db.likesSeen[key]=1;s.likes++;saveDB();broadcast({type:'stats',gameId:id,players:s.players,likes:s.likes})}return json(res,200,s)}
 if(u.pathname==='/api/follow'&&req.method==='POST'){const b=await readBody(req),a=clean(b.actor),t=clean(b.target);db.follows[a]=db.follows[a]||[];if(!db.follows[a].includes(t))db.follows[a].push(t);saveDB();return json(res,200,{ok:true})}
 if(u.pathname==='/api/friend'&&req.method==='POST'){const b=await readBody(req),a=clean(b.actor),t=clean(b.target);db.friends[a]=db.friends[a]||[];db.friends[t]=db.friends[t]||[];if(!db.friends[a].includes(t))db.friends[a].push(t);if(!db.friends[t].includes(a))db.friends[t].push(a);saveDB();return json(res,200,{ok:true})}
 if(u.pathname==='/api/block'&&req.method==='POST'){const b=await readBody(req),a=clean(b.actor),t=clean(b.target);db.blocks[t]=db.blocks[t]||[];if(!db.blocks[t].includes(a))db.blocks[t].push(a);const count=db.blocks[t].length;let isBan=false;if(count>=3){db.bans[t]=Date.now()+3*86400000;isBan=true}saveDB();return json(res,200,{ok:true,count,banned:isBan,until:db.bans[t]||0})}
 if(u.pathname==='/api/chat')return json(res,200,{messages:db.chats.slice(-200)});
 if(u.pathname.startsWith('/api/'))return json(res,404,{error:'Not found'});
 const file=u.pathname==='/'?path.join(ROOT,'NexoWorld_Arcade_MEJORADO.html'):path.join(ROOT,u.pathname.slice(1));if(!file.startsWith(ROOT)||!fs.existsSync(file))return json(res,404,{error:'Not found'});const ext=path.extname(file).toLowerCase(),mime={'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.json':'application/json; charset=utf-8','.png':'image/png','.jpg':'image/jpeg'}[ext]||'application/octet-stream';res.writeHead(200,{'Content-Type':mime,'Cache-Control':'no-cache'});fs.createReadStream(file).pipe(res)
 }catch(e){console.error(e);json(res,500,{error:'Server error'})}});
server.on('upgrade',(req,socket)=>{if(req.url!=='/ws'){socket.destroy();return}acceptWebSocket(req,socket)});
server.listen(PORT,()=>console.log('NexoWorld online: http://localhost:'+PORT));

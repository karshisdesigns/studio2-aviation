/* Studio 2 Aviation: shared save, live phone play and prize photos through Supabase.
   It offers the same small API the game used inside Claude (window.claude.use('db'|'room'|'assets')). */
(function(){
  var SB_URL='https://iviatyqsvkdtxkzdikpk.supabase.co';
  var SB_KEY='sb_publishable_hbkMBEvWz26GFWv3VBZyCA_aFev4M6-';
  if(!window.supabase||!window.supabase.createClient)return;
  var sb=window.supabase.createClient(SB_URL,SB_KEY,{auth:{persistSession:false,autoRefreshToken:false},realtime:{params:{eventsPerSecond:20}}});
  var CID=Math.random().toString(36).slice(2,10)+Date.now().toString(36);
  function err(e,code){var x=new Error((e&&e.message)||String(e));x.code=code||'unavailable';return x}

  /* ---- db: one JSON document per id, live through Postgres changes plus a slow poll as a safety net */
  function docApi(id){
    var lastStamp=null;
    function fetchRow(){return sb.from('game_docs').select('data,updated_at').eq('id',id).maybeSingle().then(function(r){if(r.error)throw err(r.error);return r.data})}
    return{
      set:function(body){
        var data=Object.assign({},body,{__by:CID});
        return sb.from('game_docs').upsert({id:id,data:data,updated_at:new Date().toISOString()}).then(function(r){if(r.error)throw err(r.error,r.error.code==='42501'?'permission_denied':'unavailable')})},
      onSnapshot:function(cb,onErr){
        var alive=true,first=true,timer=null,poll=null,ch=null;
        function emit(row){
          if(!alive)return;
          if(row&&!first&&row.updated_at===lastStamp)return;
          lastStamp=row?row.updated_at:null;
          var d=row?row.data:null,own=!first&&d&&d.__by===CID;first=false;
          cb({exists:!!d,data:function(){return d},metadata:{fromCache:false,hasPendingWrites:!!own}})}
        function pull(){return fetchRow().then(emit)}
        pull().catch(function(e){if(onErr)onErr(e)});
        ch=sb.channel('doc-'+id.replace(/[^a-z0-9]/gi,'-')+'-'+CID)
          .on('postgres_changes',{event:'*',schema:'public',table:'game_docs',filter:'id=eq.'+id},function(){clearTimeout(timer);timer=setTimeout(function(){pull().catch(function(){})},120)})
          .subscribe();
        poll=setInterval(function(){if(!document.hidden)pull().catch(function(){})},20000);
        return function(){alive=false;clearInterval(poll);if(ch)sb.removeChannel(ch)}}}
  }

  /* ---- room: broadcast messages and presence on one shared channel */
  var ROOM=null;
  function roomApi(){
    if(ROOM)return ROOM;
    var pres={},handlers={},peerHs=[],connHs=[],conn=false;
    var ch=sb.channel('studio2-room',{config:{broadcast:{self:false,ack:false},presence:{key:CID}}});
    ch.on('broadcast',{event:'*'},function(m){var p=m.payload||{};(handlers[m.event]||[]).forEach(function(h){try{h({data:p.d,from:p.f,sameTab:false})}catch(e){console.error(e)}})});
    ch.on('presence',{event:'sync'},function(){peerHs.forEach(function(h){try{h()}catch(e){}})});
    ch.subscribe(function(status){
      var was=conn;conn=status==='SUBSCRIBED';
      if(conn&&Object.keys(pres).length)ch.track(pres);
      if(was!==conn)connHs.forEach(function(h){try{h(conn)}catch(e){}})});
    ROOM={
      emit:function(t,d){return ch.send({type:'broadcast',event:t,payload:{d:d,f:CID}}).then(function(r){if(r!=='ok')throw err(r)})},
      on:function(t,h){(handlers[t]=handlers[t]||[]).push(h);return function(){handlers[t]=handlers[t].filter(function(x){return x!==h})}},
      presence:function(patch){pres=Object.assign({},pres,patch);return conn?ch.track(pres):Promise.resolve()},
      peers:function(){var st=ch.presenceState();return Object.keys(st).map(function(k){var m=st[k]||[];return{peer:k,presence:m[m.length-1]||{},sameTab:k===CID}})},
      onPeers:function(h){peerHs.push(h);return function(){peerHs=peerHs.filter(function(x){return x!==h})}},
      connected:function(){return conn},
      onConnection:function(h){connHs.push(h);return function(){connHs=connHs.filter(function(x){return x!==h})}}};
    /* keep the board tab alive when a laptop sleeps and wakes */
    document.addEventListener('visibilitychange',function(){if(!document.hidden&&conn&&Object.keys(pres).length)ch.track(pres)});
    return ROOM}

  /* ---- assets: prize photos go to a public storage bucket, shrunk to 1400px first */
  function shrink(file){return new Promise(function(res){
    if(!/^image\/(jpeg|png|webp)$/.test(file.type)){res(file);return}
    var img=new Image(),u=URL.createObjectURL(file);
    img.onload=function(){var s=Math.min(1,1400/Math.max(img.width,img.height)),c=document.createElement('canvas');c.width=Math.round(img.width*s);c.height=Math.round(img.height*s);
      c.getContext('2d').drawImage(img,0,0,c.width,c.height);URL.revokeObjectURL(u);c.toBlob(function(b){res(b||file)},'image/jpeg',.86)};
    img.onerror=function(){URL.revokeObjectURL(u);res(file)};img.src=u})}
  var assetsApi={upload:function(file){return shrink(file).then(function(b){
    var path='photos/'+Date.now()+'-'+Math.random().toString(36).slice(2,8)+'.jpg';
    return sb.storage.from('prizes').upload(path,b,{contentType:b.type||'image/jpeg',upsert:false}).then(function(r){if(r.error)throw err(r.error);
      return{url:sb.storage.from('prizes').getPublicUrl(path).data.publicUrl}})})}};

  window.claude={use:function(name){return Promise.resolve(name==='db'?{doc:docApi}:name==='room'?roomApi():name==='assets'?assetsApi:null)}};
})();

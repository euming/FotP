(function(){
  function escRx(s){return s.replace(/[.*+?^${}()|[\]\\]/g,'\\$&');}
  function sourceBucket(source){
    var s=(source||'').toLowerCase();
    if(s==='claude'||s==='codex')return s;
    return 'other';
  }
  function applyHighlight(sessionEl,kw){
    var bubbles=sessionEl.querySelectorAll('.bub-text');
    bubbles.forEach(function(b){if(b.dataset.rawHtml)b.innerHTML=b.dataset.rawHtml;});
    if(!kw)return null;
    var rx=new RegExp('('+escRx(kw)+')','gi');
    var first=true;
    bubbles.forEach(function(b){
      if(!b.dataset.rawHtml)b.dataset.rawHtml=b.innerHTML;
      b.innerHTML=b.dataset.rawHtml.replace(rx,function(m){
        var cls=first?'kw-hl kw-first':'kw-hl'; first=false;
        return '<mark class="'+cls+'">'+m+'</mark>';
      });
    });
    return sessionEl.querySelector('mark.kw-first');
  }
  function navToHash(hash,kw){
    if(!hash||hash==='#')return;
    var el=document.querySelector(hash);
    if(!el)return;
    if(el.tagName==='DETAILS')el.open=true;
    var p=el; while(p){if(p.tagName==='DETAILS')p.open=true; p=p.parentElement;}
    setTimeout(function(){
      var scrollTo=el;
      if(kw){var m=applyHighlight(el,kw); if(m)scrollTo=m;}
      scrollTo.scrollIntoView({block:'start'});
      el.classList.remove('hl-flash'); void el.offsetWidth; el.classList.add('hl-flash');
    },150);
  }
  function hasVisibleSession(root){
    return root.querySelector('details.session[data-source]:not([hidden])')!==null;
  }
  function applyTimelineSourceFilter(filter){
    var sessions=document.querySelectorAll('details.session[data-source]');
    sessions.forEach(function(session){
      var source=(session.dataset.source||'').toLowerCase();
      var show=(filter==='all')||(sourceBucket(source)===filter);
      session.hidden=!show;
    });

    var segments=document.querySelectorAll('details.tl-segment');
    segments.forEach(function(seg){ seg.hidden=!hasVisibleSession(seg); });

    var days=document.querySelectorAll('details.tl-day');
    days.forEach(function(day){ day.hidden=!hasVisibleSession(day); });

    var months=document.querySelectorAll('details.tl-month');
    months.forEach(function(month){ month.hidden=!hasVisibleSession(month); });

    var years=document.querySelectorAll('details.tl-year');
    years.forEach(function(year){ year.hidden=!hasVisibleSession(year); });
  }
  function setActiveFilter(filter){
    var buttons=document.querySelectorAll('.tl-src-btn');
    buttons.forEach(function(btn){
      var on=btn.dataset.filter===filter;
      btn.classList.toggle('is-active',on);
    });
    applyTimelineSourceFilter(filter);
  }
  document.addEventListener('click',function(e){
    var filterBtn=e.target.closest('.tl-src-btn');
    if(filterBtn){
      e.preventDefault();
      setActiveFilter(filterBtn.dataset.filter||'all');
      return;
    }
    var a=e.target.closest('a[href^="#"]');
    if(!a)return;
    var hash=a.getAttribute('href');
    if(!hash||hash==='#')return;
    e.preventDefault();
    history.pushState(null,'',hash);
    navToHash(hash,a.dataset.highlight||null);
  });
  window.addEventListener('popstate',function(){navToHash(window.location.hash);});
  if(window.location.hash)setTimeout(function(){navToHash(window.location.hash);},0);
  setActiveFilter('all');
})();

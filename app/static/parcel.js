const ui={message:document.querySelector('#message'),sample:document.querySelector('#sample-name'),file:document.querySelector('#scan-file')};async function requestRun(url,body,isFile=false){ui.message.textContent='正在核算…';try{const options={method:'POST',body:isFile?body:JSON.stringify(body)};if(!isFile)options.headers={'Content-Type':'application/json'};const res=await fetch(url,options);const result=await res.json();if(!res.ok)throw new Error(result.message);window.location.reload();}catch(e){ui.message.textContent=e.message;}}document.querySelector('#retry').addEventListener('click',()=>requestRun('/api/runs',{sample:ui.sample.value}));document.querySelector('#read-file').addEventListener('click',()=>{if(!ui.file.files.length){ui.message.textContent='请选择扫描文件。';return;}const data=new FormData();data.append('file',ui.file.files[0]);requestRun('/api/upload',data,true);});document.querySelector('#review-run').addEventListener('click',()=>requestRun('/api/runs/'+window.parcelRun.id+'/review',{version:window.parcelRun.version,decision:'已复核'}));
const batchInput=document.querySelector('#batch-files');
if(batchInput){document.querySelector('#ingest-batch').addEventListener('click',async()=>{
  const note=document.querySelector('#batch-message');
  if(!batchInput.files.length){note.textContent='请选择一个或多个同日扫描文件。';return;}
  note.textContent='正在采信并重建全集…';
  try{
    const data=new FormData();
    for(const f of batchInput.files){data.append('files',f);}
    const res=await fetch('/api/batches',{method:'POST',body:data});
    const body=await res.json();
    if(!res.ok)throw new Error(body.message);
    note.textContent=(body.reused_snapshot?'与既有全集一致，未新增扫描。':'全集已切换，当前扫描数 '+body.stages[0].metrics.canonical_scans+'。');
    setTimeout(()=>window.location.reload(),900);
  }catch(e){note.textContent='采信失败，已保留上一可用全集：'+e.message;}
});}
document.querySelector('#search-scans')?.addEventListener('input',event=>{document.querySelectorAll('#scan-rows tr').forEach(row=>{row.hidden=!row.textContent.includes(event.target.value);});});
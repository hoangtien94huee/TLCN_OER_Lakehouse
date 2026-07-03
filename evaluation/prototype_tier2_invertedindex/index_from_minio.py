import os, json, time, io, sys
import boto3
import requests
from pypdf import PdfReader

ES="http://elasticsearch:9200"; INDEX="oer_pages_tier2"; BUCKET="oer-lakehouse"
MIN_CHARS=40
def clean(s):
    return s.encode("utf-8","ignore").decode("utf-8","ignore")
s3=boto3.client("s3",endpoint_url="http://minio:9000",
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY","minioadmin"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY","minioadmin"))
docs=json.load(open("/tmp/docs_map.json")); N=len(docs)
def indexed(uid):
    r=requests.post(f"{ES}/{INDEX}/_count",json={"query":{"term":{"asset_uid":uid}}},timeout=30)
    return r.status_code==200 and (r.json().get("count") or 0)>0
def extract(obj):
    """Return list of (g,page_no,text). Fully guarded against corrupt PDFs."""
    pages=[]; g=0
    try:
        reader=PdfReader(io.BytesIO(obj))
        for pno,page in enumerate(reader.pages):
            try: txt=clean((page.extract_text() or "").strip())
            except Exception: txt=""
            if len(txt)>=MIN_CHARS: pages.append((g,pno,txt)); g+=1
    except Exception:
        return None   # corrupt/unreadable
    return pages
def bulk(uid,title,pages,batch=500):
    title=clean(title)
    for i in range(0,len(pages),batch):
        ch=pages[i:i+batch]; lines=[]
        for pg in ch:
            lines.append(json.dumps({"index":{"_index":INDEX,"_id":f"{uid}:{pg[0]}"}}))
            lines.append(json.dumps({"asset_uid":uid,"title":title,"page_no":pg[1],
                "global_idx":pg[0],"chapter_title":None,"section_title":None,"text":pg[2]},ensure_ascii=False))
        rr=requests.post(f"{ES}/_bulk",data=("\n".join(lines)+"\n").encode("utf-8","ignore"),
            headers={"Content-Type":"application/x-ndjson"},timeout=120); rr.raise_for_status()
t0=time.time(); done=skip=scan=err=grand=0
for i,(uid,v) in enumerate(docs.items(),1):
    try:
        if indexed(uid): skip+=1; continue
        ap=v.get("asset_path"); title=v.get("title") or ""
        if not ap: continue
        try:
            obj=s3.get_object(Bucket=BUCKET,Key=ap)["Body"].read()
        except Exception: err+=1; continue
        pages=extract(obj)
        if pages is None: err+=1; continue
        if not pages: scan+=1; continue
        bulk(uid,title,pages); done+=1; grand+=len(pages)
        if done%50==0:
            print(f"[{i}/{N}] done={done} scan={scan} err={err} | {grand}tr | {time.time()-t0:.0f}s | {title[:30]}",flush=True)
    except Exception as e:
        err+=1; print(f"  ! {uid[:10]} unexpected: {str(e)[:70]}",flush=True); continue
requests.post(f"{ES}/{INDEX}/_refresh",timeout=60)
cnt=requests.get(f"{ES}/{INDEX}/_count",timeout=30).json().get("count")
print(f"DONE. index mới {done} sách / {grand}tr | bỏ scan {scan} | đã có {skip} | lỗi {err} | index giữ {cnt}tr | {time.time()-t0:.0f}s",flush=True)

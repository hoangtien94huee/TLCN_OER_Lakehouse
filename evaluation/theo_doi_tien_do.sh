#!/bin/bash
# Theo dõi tiến độ eval EN/VI (RAGAS) + thí nghiệm Google Translate.
# Dùng:  bash theo_doi_tien_do.sh          (xem 1 lần)
#        watch -n 30 bash theo_doi_tien_do.sh   (tự cập nhật mỗi 30s, Ctrl+C để thoát)
SP=/tmp/claude-1000/-home-lib-oer-chatbot-project/364663b5-8e25-46f2-a877-37c9235a4fa8/scratchpad

echo "===== RAGAS EN/VI (generation) — $(date +%H:%M:%S) ====="
done_n=0
for lang in en vi; do
  for skip in 0 12 24 36 48 60; do
    f=$SP/ragas_${lang}_c${skip}.json
    if [ -s "$f" ]; then
      done_n=$((done_n+1))
      python3 -c "import json;d=json.load(open('$f'));print(f'  [XONG] ${lang} c${skip}: faithfulness={d[\"faithfulness\"]}  relevancy={d[\"answer_relevancy\"]}  (n={d[\"n\"]})')"
    else
      # đang chạy? xem log có % không + log có mới không (3 phút)
      lg=$SP/log_${lang}_c${skip}.txt
      pct=$(tail -c 400 $lg 2>/dev/null | tr '\r' '\n' | grep -oE "Evaluating: +[0-9]+%" | tail -1)
      if [ -n "$pct" ]; then
        if [ -n "$(find $lg -mmin -3 2>/dev/null)" ]; then
          echo "  [CHẠY] ${lang} c${skip}: $pct"
        else
          echo "  [IM ẮNG] ${lang} c${skip}: $pct (log cũ >3ph — chờ retry hoặc bị cắt)"
        fi
      else
        echo "  [CHỜ ] ${lang} c${skip}"
      fi
    fi
  done
done
echo "  → Tổng: $done_n/12 lô xong"

echo ""
echo "===== Thí nghiệm dịch Google vs dịch LLM (retrieval) ====="
for f in pipeline_vi_control_today:"Đối chứng (LLM hệ dịch, hôm nay)" pipeline_vi_google_v2:"Google dịch (bản chuẩn v2)"; do
  name=${f%%:*}; label=${f#*:}
  if [ -s "$SP/$name.json" ]; then
    python3 -c "
import json
d = json.load(open('$SP/$name.json'))
ok = sum(1 for r in d if not r.get('error'))
print(f'  $label: {len(d)}/61 câu ({ok} OK)')"
  else
    echo "  $label: chưa chạy / đang khởi động"
  fi
done
tail -2 /home/lib/oer_chatbot_project/TLCN_OER_Lakehouse/evaluation/scripts/logs/02_run_pipeline.log 2>/dev/null | sed 's/^/  log: /'

echo ""
echo "===== Nhật ký supervisor (RAGAS) ====="
tail -4 /tmp/claude-1000/-home-lib-oer-chatbot-project/364663b5-8e25-46f2-a877-37c9235a4fa8/tasks/br9r0dm1c.output 2>/dev/null | sed 's/^/  /'

echo ""
echo "===== Tiến trình đang sống ====="
echo "  RAGAS: $(ps aux | grep -c '[r]agas_eval.py' ) tiến trình  |  Pipeline: $(ps aux | grep -c '[r]un_pipeline.py') tiến trình"

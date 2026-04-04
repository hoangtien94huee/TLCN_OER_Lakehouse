#!/bin/bash
# Export TOÀN BỘ chunks (không limit)

echo "=================================================="
echo "🚀 Export TOÀN BỘ chunks (ALL)"
echo "=================================================="

OUTPUT_FILE="/tmp/chunks_all.jsonl"

echo "📊 Exporting ALL chunks (no limit)..."
echo "⏱️  Dự kiến: 5-10 phút cho ~160K chunks"
echo ""

# Copy script vào container
docker cp scripts/export_chunks_for_colab.py oer-spark-master:/tmp/

# Chạy export KHÔNG CÓ --limit
docker exec oer-spark-master spark-submit \
    --master local[*] \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.565 \
    /tmp/export_chunks_for_colab.py \
    --output ${OUTPUT_FILE}

if [ $? -ne 0 ]; then
    echo "❌ Export failed!"
    exit 1
fi

# Copy file ra ngoài
echo ""
echo "📥 Copying file to host..."
docker cp oer-spark-master:${OUTPUT_FILE} ./data/chunks_all.jsonl

echo ""
echo "✅ DONE!"
echo "=================================================="
echo "📁 File: data/chunks_all.jsonl"
ls -lh data/chunks_all.jsonl 2>/dev/null || echo "File not found"
wc -l data/chunks_all.jsonl 2>/dev/null | awk '{print "Total chunks: " $1}'
echo "=================================================="

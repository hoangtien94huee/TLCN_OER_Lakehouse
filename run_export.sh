#!/bin/bash
# Script để export chunks sử dụng spark-submit

echo "=================================================="
echo "🚀 Export Chunks để đưa lên Colab"
echo "=================================================="

# Số lượng chunks cần export (mặc định 1000 để test)
LIMIT=${1:-1000}
OUTPUT_FILE="/tmp/chunks_for_colab.jsonl"

echo "📊 Exporting ${LIMIT} chunks..."
echo ""

# Copy script vào container
docker cp scripts/export_chunks_for_colab.py oer-spark-master:/tmp/

# Chạy export bằng spark-submit (có đầy đủ environment)
docker exec oer-spark-master spark-submit \
    --master local[*] \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.565 \
    /tmp/export_chunks_for_colab.py \
    --output ${OUTPUT_FILE} \
    --limit ${LIMIT}

if [ $? -ne 0 ]; then
    echo "❌ Export failed!"
    exit 1
fi

# Copy file ra ngoài
echo ""
echo "📥 Copying file to host..."
docker cp oer-spark-master:${OUTPUT_FILE} ./data/chunks_for_colab.jsonl

echo ""
echo "✅ DONE!"
echo "=================================================="
echo "📁 File: data/chunks_for_colab.jsonl"
ls -lh data/chunks_for_colab.jsonl 2>/dev/null || echo "File not found"
echo "=================================================="
echo ""
echo "🔥 Bước tiếp theo:"
echo "1. File này là JSONL (mỗi dòng 1 JSON)"
echo "2. Upload lên Colab"
echo "3. Chạy code embedding (xem COLAB_QUICKSTART.md)"
echo ""

#!/bin/bash

###############################################################################
# DSpace SAF Batch Import Script
# 
# Usage:
#   ./import_saf_batch.sh <zip_file> [batch_size] [collection_uuid]
#
# Examples:
#   ./import_saf_batch.sh saf_export.zip
#   ./import_saf_batch.sh saf_export.zip 500
#   ./import_saf_batch.sh saf_export.zip 400 9f3ea5df-1a63-463f-89b8-ec00d7dcf50a
#
# Default values:
#   - batch_size: 400 items per batch
#   - collection_uuid: 9f3ea5df-1a63-463f-89b8-ec00d7dcf50a (oer collection)
#   - admin_email: admin@dspace.org
#
###############################################################################

set -e

# ============================================================================
# Configuration
# ============================================================================

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
BATCH_SIZE=400
COLLECTION_UUID="9f3ea5df-1a63-463f-89b8-ec00d7dcf50a"  # OER collection
ADMIN_EMAIL="admin@gmail.com"
DSPACE_CONTAINER="dspace"

# Parse arguments
SAF_ZIP_FILE="$1"
if [ -n "$2" ]; then BATCH_SIZE="$2"; fi
if [ -n "$3" ]; then COLLECTION_UUID="$3"; fi

# ============================================================================
# Functions
# ============================================================================

log_info() {
  echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
  echo -e "${GREEN}[✓]${NC} $1"
}

log_error() {
  echo -e "${RED}[ERROR]${NC} $1"
}

log_warning() {
  echo -e "${YELLOW}[WARNING]${NC} $1"
}

progress_bar() {
  local current=$1
  local total=$2
  local width=40
  local percentage=$((current * 100 / total))
  local filled=$((percentage * width / 100))
  
  printf "Progress: ["
  printf "%${filled}s" | tr ' ' '='
  printf "%$((width - filled))s" | tr ' ' '-'
  printf "] %d%% (%d/%d)\n" "$percentage" "$current" "$total"
}

# ============================================================================
# Validation
# ============================================================================

validate_inputs() {
  if [ -z "$SAF_ZIP_FILE" ]; then
    log_error "No SAF zip file provided"
    echo "Usage: $0 <zip_file> [batch_size] [collection_uuid]"
    exit 1
  fi

  if [ ! -f "$SAF_ZIP_FILE" ]; then
    log_error "File not found: $SAF_ZIP_FILE"
    exit 1
  fi

  if [ ! -f "$SAF_ZIP_FILE" ] || [ "${SAF_ZIP_FILE##*.}" != "zip" ]; then
    log_error "Input must be a .zip file"
    exit 1
  fi

  # Check if DSpace container is running
  if ! docker ps | grep -q "$DSPACE_CONTAINER"; then
    log_error "DSpace container '$DSPACE_CONTAINER' is not running"
    echo "Start it with: docker-compose up -d"
    exit 1
  fi

  if ! command -v unzip &> /dev/null; then
    log_error "unzip command not found. Install it with: apt-get install unzip"
    exit 1
  fi
}

# ============================================================================
# Extract SAF
# ============================================================================

extract_saf() {
  log_info "Extracting SAF file: $SAF_ZIP_FILE"
  
  local extract_dir=$(basename "$SAF_ZIP_FILE" .zip)_extracted
  
  if [ -d "$extract_dir" ]; then
    log_warning "Directory '$extract_dir' already exists. Removing old one..."
    rm -rf "$extract_dir"
  fi
  
  mkdir -p "$extract_dir"
  unzip -q "$SAF_ZIP_FILE" -d "$extract_dir"
  
  local item_count=$(ls -d "$extract_dir"/item_* 2>/dev/null | wc -l)
  
  if [ "$item_count" -eq 0 ]; then
    log_error "No items found in extracted SAF file"
    exit 1
  fi
  
  log_success "Extracted $item_count items to $extract_dir"
  echo "$extract_dir"
}

# ============================================================================
# Split into Batches
# ============================================================================

split_into_batches() {
  local saf_dir="$1"
  local batch_size="$2"
  
  log_info "Splitting items into batches of $batch_size..."
  
  local items=( "$saf_dir"/item_* )
  local total_items=${#items[@]}
  local num_batches=$(( (total_items + batch_size - 1) / batch_size ))
  
  log_info "Total items: $total_items"
  log_info "Batch size: $batch_size"
  log_info "Number of batches: $num_batches"
  
  # Create batch directories
  for ((i=1; i<=num_batches; i++)); do
    local batch_dir="saf_batch_$i"
    
    if [ -d "$batch_dir" ]; then
      rm -rf "$batch_dir"
    fi
    mkdir -p "$batch_dir"
    
    # Calculate start and end indices
    local start=$(( (i-1) * batch_size ))
    local count=$batch_size
    
    # Copy items to batch directory
    for ((j=0; j<count && start+j<total_items; j++)); do
      cp -r "${items[$((start+j))]}" "$batch_dir/"
    done
    
    local batch_count=$(ls -d "$batch_dir"/item_* 2>/dev/null | wc -l)
    log_success "Created batch $i: $batch_dir ($batch_count items)"
    progress_bar "$i" "$num_batches"
  done
  
  echo "$num_batches"
}

# ============================================================================
# Import Batches
# ============================================================================

import_batch() {
  local batch_num=$1
  local batch_dir=$2
  local total_batches=$3
  
  log_info "Importing batch $batch_num/$total_batches from $batch_dir..."
  
  local item_count=$(ls -d "$batch_dir"/item_* 2>/dev/null | wc -l)
  local container_batch_dir="/tmp/saf_batch_import_$batch_num"
  
  # Copy batch to container
  log_info "  Copying batch to DSpace container..."
  if ! docker cp "$batch_dir"/* "$DSPACE_CONTAINER:$container_batch_dir/" 2>/dev/null; then
    docker exec "$DSPACE_CONTAINER" mkdir -p "$container_batch_dir"
    docker cp "$batch_dir"/* "$DSPACE_CONTAINER:$container_batch_dir/"
  fi
  
  # Run import
  log_info "  Starting import ($item_count items)..."
  local mapfile="/tmp/mapfile_batch_$batch_num.txt"
  local logfile="/tmp/import_batch_$batch_num.log"
  
  docker exec "$DSPACE_CONTAINER" bash << DOCKER_EOF
    cd /dspace
    
    # Import batch
    ./bin/dspace dsrun org.dspace.app.itemimport.ItemImport \
      -a \
      -e $ADMIN_EMAIL \
      -s $container_batch_dir \
      -c $COLLECTION_UUID \
      -m $mapfile \
      > $logfile 2>&1
    
    # Check result
    if [ -f $mapfile ]; then
      IMPORTED=\$(wc -l < $mapfile)
      echo "Imported \$IMPORTED items"
    else
      echo "Import may have failed - check $logfile"
    fi
DOCKER_EOF
  
  local exit_code=$?
  
  if [ $exit_code -eq 0 ]; then
    log_success "Batch $batch_num imported successfully"
  else
    log_warning "Batch $batch_num import may have issues. Check logs: docker exec $DSPACE_CONTAINER tail -50 $logfile"
  fi
  
  # Show progress
  progress_bar "$batch_num" "$total_batches"
  
  # Small delay between batches to avoid overwhelming database
  if [ "$batch_num" -lt "$total_batches" ]; then
    sleep 5
  fi
  
  return $exit_code
}

# ============================================================================
# Generate Report
# ============================================================================

generate_report() {
  local num_batches=$1
  
  log_info "Generating import report..."
  
  local report_file="import_report_$(date +%Y%m%d_%H%M%S).txt"
  
  {
    echo "==============================================="
    echo "DSpace SAF Batch Import Report"
    echo "==============================================="
    echo "Date: $(date)"
    echo "Input File: $SAF_ZIP_FILE"
    echo "Batch Size: $BATCH_SIZE"
    echo "Collection UUID: $COLLECTION_UUID"
    echo "Number of Batches: $num_batches"
    echo ""
    echo "Import Results:"
    echo "---"
    
    for ((i=1; i<=num_batches; i++)); do
      local mapfile="/tmp/mapfile_batch_$i.txt"
      docker exec "$DSPACE_CONTAINER" cat "$mapfile" 2>/dev/null | wc -l | xargs -I {} echo "  Batch $i: {} items imported"
    done
    
    echo ""
    echo "Verification Commands:"
    echo "  Check in UI: http://localhost:4000/handle/123456789/2"
    echo "  Check via API: curl http://localhost:8180/server/api/core/collections/$COLLECTION_UUID/items"
    echo "  View logs: docker exec $DSPACE_CONTAINER tail -50 /tmp/import_batch_1.log"
    echo ""
    echo "==============================================="
  } | tee "$report_file"
  
  log_success "Report saved to: $report_file"
}

# ============================================================================
# Cleanup
# ============================================================================

cleanup() {
  if [ "$1" = "keep" ]; then
    log_info "Keeping batch directories for reference"
  else
    log_info "Cleaning up temporary batch directories..."
    for dir in saf_batch_*/; do
      if [ -d "$dir" ]; then
        log_warning "  Removing $dir (set KEEP_BATCHES=1 to keep)"
        # Note: commented out to prevent accidental deletion
        # rm -rf "$dir"
      fi
    done
  fi
}

# ============================================================================
# Main
# ============================================================================

main() {
  echo ""
  echo "═════════════════════════════════════════════════════════════"
  echo "  DSpace SAF Batch Import Script"
  echo "═════════════════════════════════════════════════════════════"
  echo ""
  
  # Validate inputs
  validate_inputs
  
  # Display configuration
  log_info "Configuration:"
  echo "  ZIP File: $SAF_ZIP_FILE"
  echo "  Batch Size: $BATCH_SIZE"
  echo "  Collection UUID: $COLLECTION_UUID"
  echo "  Admin Email: $ADMIN_EMAIL"
  echo "  DSpace Container: $DSPACE_CONTAINER"
  echo ""
  
  # Extract SAF
  local saf_dir=$(extract_saf)
  echo ""
  
  # Split into batches
  local num_batches=$(split_into_batches "$saf_dir" "$BATCH_SIZE")
  echo ""
  
  # Confirm before importing
  read -p "Ready to import $num_batches batches. Continue? (y/n) " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    log_warning "Import cancelled by user"
    exit 0
  fi
  
  # Import batches
  log_info "Starting batch imports..."
  echo ""
  
  local failed_batches=()
  for ((i=1; i<=num_batches; i++)); do
    if ! import_batch "$i" "saf_batch_$i" "$num_batches"; then
      failed_batches+=($i)
    fi
  done
  
  echo ""
  
  # Generate report
  generate_report "$num_batches"
  
  # Show results
  echo ""
  if [ ${#failed_batches[@]} -eq 0 ]; then
    log_success "All batches imported successfully!"
  else
    log_warning "Some batches had issues: ${failed_batches[@]}"
  fi
  
  # Cleanup
  echo ""
  cleanup "keep"
  
  echo ""
  echo "═════════════════════════════════════════════════════════════"
  echo "  Import Complete"
  echo "═════════════════════════════════════════════════════════════"
  echo ""
  echo "Next steps:"
  echo "  1. Verify in DSpace UI: http://localhost:4000/handle/123456789/2"
  echo "  2. Check search index: docker exec $DSPACE_CONTAINER ./bin/dspace index-discovery"
  echo "  3. Monitor logs: docker exec $DSPACE_CONTAINER tail -f /tmp/import_batch_1.log"
  echo ""
}

# Run main function
main "$@"

#!/bin/sh

# -------------------------------------------------
# Fail on unset variables and command errors
# -------------------------------------------------
set -eu

# -------------------------------------------------
# Configuration
# -------------------------------------------------
SCRIPT_NAME=$(basename "$0")
SCRIPT_DIR="/data/informatica/ming/Scripts"
LOG_DIR="/data/informatica/ming/ScriptsLogs"

BASE_DIR="/data/data2/staging/CREDIT_CARDS/BNS"

# -------------------------------------------------
# Validate input
# -------------------------------------------------
if [ "$#" -ne 1 ]; then
  echo "Usage: ${SCRIPT_NAME} <FILE_TYPE>"
  exit 1
fi

FILE_TYPE="$1"

CTR_PATTERN="${BASE_DIR}/${FILE_TYPE}_*.csv.ctr"
DATA_PATTERN="${BASE_DIR}/DETOKENIZED/${FILE_TYPE}*.csv"

# -------------------------------------------------
# Logging setup
# -------------------------------------------------
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
LOG_FILE="${LOG_DIR}/${SCRIPT_NAME%.sh}_${FILE_TYPE}_${TIMESTAMP}.log"

mkdir -p "${LOG_DIR}"

# Redirect stdout and stderr to log file AND console
exec 3>&1 4>&2
exec >"${LOG_FILE}" 2>&1

log() {
  printf '%s | %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

fail() {
  log "ERROR: $*"
  exit 1
}

# -------------------------------------------------
# Script start
# -------------------------------------------------
log "=============================================="
log "Script started"
log "Script name : ${SCRIPT_NAME}"
log "File type   : ${FILE_TYPE}"
log "=============================================="

# -------------------------------------------------
# Resolve CTR file
# -------------------------------------------------
log "Resolving CTR file using pattern: ${CTR_PATTERN}"

CTR_FILE=$(ls ${CTR_PATTERN} 2>/dev/null) || fail "No CTR file found"

log "CTR file resolved: ${CTR_FILE}"

# -------------------------------------------------
# Calculate SHA-512 checksum
# -------------------------------------------------
log "Calculating SHA-512 checksum"

SHA_VALUE=$(sha512sum ${DATA_PATTERN} 2>/dev/null | cut -d ' ' -f 1)

[ -n "${SHA_VALUE}" ] || fail "Checksum calculation failed"

log "Checksum calculated successfully"

# -------------------------------------------------
# Step 1: Update checksum column
# -------------------------------------------------
log "Updating column 6 (CheckSum)"

gawk -F',' -v OFS="," -v SHA_VALUE="${SHA_VALUE}" '
{
  $6 = SHA_VALUE
  print
}
' "${CTR_FILE}" > "${CTR_FILE}.tmp" || fail "Failed updating checksum"

mv -f "${CTR_FILE}.tmp" "${CTR_FILE}"

log "Checksum column updated"

# -------------------------------------------------
# Step 2: Convert CSV to key:value format
# -------------------------------------------------
log "Converting CTR file to key:value format"

gawk -F',' '
BEGIN {
  column_names[1] = "SourceName"
  column_names[2] = "DataFileName"
  column_names[3] = "BusinessEffectiveDate"
  column_names[4] = "RecordCount"
  column_names[5] = "CheckSumType"
  column_names[6] = "CheckSum"
}
{
  for (i = 1; i <= NF; i++) {
    print column_names[i] ":" $i
  }
}
' "${CTR_FILE}" > "${CTR_FILE}.tmp" || fail "Failed formatting CTR file"

mv -f "${CTR_FILE}.tmp" "${CTR_FILE}"

log "CTR file formatting completed"

# -------------------------------------------------
# Script end
# -------------------------------------------------
log "Script completed successfully"
log "=============================================="

# Restore stdout/stderr
exec 1>&3 2>&4

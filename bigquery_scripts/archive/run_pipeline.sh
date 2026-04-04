#!/bin/bash
# BigQuery Pipeline Setup Script
# This script sets up and runs the BigQuery finance data pipeline

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ID="${GOOGLE_CLOUD_PROJECT}"
DATASET_ID="${1:-finance_data}"
BUCKET_NAME="${GCP_GCS_BUCKET}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}BigQuery Finance Data Pipeline${NC}"
echo -e "${BLUE}========================================${NC}"

# Check environment variables
if [ -z "$PROJECT_ID" ]; then
    echo -e "${RED}Error: GOOGLE_CLOUD_PROJECT not set${NC}"
    echo "Please set: export GOOGLE_CLOUD_PROJECT=your-project-id"
    exit 1
fi

if [ -z "$BUCKET_NAME" ]; then
    echo -e "${RED}Error: GCP_GCS_BUCKET not set${NC}"
    echo "Please set: export GCP_GCS_BUCKET=your-bucket-name"
    exit 1
fi

echo -e "${GREEN}✓ Environment Configuration:${NC}"
echo "  Project ID: $PROJECT_ID"
echo "  Dataset ID: $DATASET_ID"
echo "  Bucket: $BUCKET_NAME"
echo ""

# Check Python and pip
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: Python 3 is not installed${NC}"
    exit 1
fi

# Install dependencies
echo -e "${BLUE}Installing dependencies...${NC}"
pip install -q -r "$SCRIPT_DIR/requirements.txt"
echo -e "${GREEN}✓ Dependencies installed${NC}"
echo ""

# Run pipeline
echo -e "${BLUE}Starting BigQuery pipeline...${NC}"
python3 "$SCRIPT_DIR/bigquery_orchestrator.py" \
    --project-id "$PROJECT_ID" \
    --dataset-id "$DATASET_ID" \
    --bucket-name "$BUCKET_NAME"

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}Pipeline completed successfully!${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo "Next steps:"
    echo "1. Check tables in BigQuery console"
    echo "2. Run sample queries from README.md"
    echo "3. Set up scheduled queries for regular updates"
else
    echo ""
    echo -e "${RED}Pipeline failed. Check logs above.${NC}"
    exit 1
fi

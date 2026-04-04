#!/bin/bash
# Deploy and run BigQuery pipeline locally or in Cloud Run

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_usage() {
    echo "Usage: $0 [OPTION]"
    echo "Options:"
    echo "  local       Run pipeline locally"
    echo "  setup       Setup Python environment locally"
    echo "  verify      Verify GCP credentials and configuration"
    echo "  validate    Validate CSV files in GCS"
    echo "  help        Show this help message"
}

setup_local_environment() {
    echo -e "${BLUE}Setting up local Python environment...${NC}"
    
    # Check if venv exists
    if [ ! -d "$SCRIPT_DIR/venv" ]; then
        echo "Creating virtual environment..."
        python3 -m venv "$SCRIPT_DIR/venv"
    fi
    
    # Activate venv
    echo "Activating virtual environment..."
    source "$SCRIPT_DIR/venv/bin/activate"
    
    # Install dependencies
    echo "Installing dependencies..."
    pip install -q --upgrade pip
    pip install -q -r "$SCRIPT_DIR/requirements.txt"
    
    echo -e "${GREEN}✓ Local environment setup complete${NC}"
    echo ""
    echo "To activate the environment, run:"
    echo "  source $SCRIPT_DIR/venv/bin/activate"
}

verify_credentials() {
    echo -e "${BLUE}Verifying GCP credentials...${NC}"
    
    # Check if GOOGLE_APPLICATION_CREDENTIALS is set
    if [ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
        echo -e "${YELLOW}GOOGLE_APPLICATION_CREDENTIALS not set${NC}"
        echo "Checking for Application Default Credentials..."
        
        if gcloud auth application-default print-access-token &> /dev/null; then
            echo -e "${GREEN}✓ Application Default Credentials found${NC}"
        else
            echo -e "${RED}✗ No credentials found${NC}"
            echo ""
            echo "To set up credentials, run one of:"
            echo "  1. gcloud auth application-default login"
            echo "  2. export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json"
            return 1
        fi
    else
        if [ ! -f "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
            echo -e "${RED}✗ Service account key file not found: $GOOGLE_APPLICATION_CREDENTIALS${NC}"
            return 1
        fi
        echo -e "${GREEN}✓ Service account key found${NC}"
    fi
    
    # Test BigQuery connection
    echo "Testing BigQuery connection..."
    if gcloud bq ls --project_id="$GOOGLE_CLOUD_PROJECT" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ BigQuery connection successful${NC}"
    else
        echo -e "${RED}✗ BigQuery connection failed${NC}"
        return 1
    fi
    
    # Test GCS connection
    echo "Testing GCS connection..."
    if gsutil ls "gs://$GCP_GCS_BUCKET" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ GCS bucket accessible${NC}"
    else
        echo -e "${RED}✗ GCS bucket not accessible${NC}"
        return 1
    fi
    
    echo -e "${GREEN}✓ All credentials verified${NC}"
}

validate_csv_files() {
    echo -e "${BLUE}Validating CSV files in GCS...${NC}"
    
    files=(
        "$CSV_BALANCE_SHEET"
        "$CSV_INCOME_STATEMENT"
        "$CSV_COMPANY_INFO"
        "$CSV_CASH_FLOW"
    )
    
    all_exist=true
    for file in "${files[@]}"; do
        if gsutil ls "gs://$GCP_GCS_BUCKET/$file" &> /dev/null; then
            size=$(gsutil du "gs://$GCP_GCS_BUCKET/$file" | awk '{print $1}')
            echo -e "${GREEN}✓ $file${NC} ($(numfmt --to=iec-i --suffix=B $size 2>/dev/null || echo $size bytes))"
        else
            echo -e "${RED}✗ $file${NC} - NOT FOUND"
            all_exist=false
        fi
    done
    
    if [ "$all_exist" = true ]; then
        echo -e "${GREEN}✓ All CSV files found${NC}"
        return 0
    else
        echo -e "${RED}✗ Some CSV files are missing${NC}"
        return 1
    fi
}

run_local_pipeline() {
    echo -e "${BLUE}Running BigQuery pipeline...${NC}"
    echo ""
    
    # Check if venv needs to be activated
    if [ -z "$VIRTUAL_ENV" ]; then
        source "$SCRIPT_DIR/venv/bin/activate"
    fi
    
    # Load environment variables from .env
    if [ -f "$SCRIPT_DIR/.env" ]; then
        echo "Loading environment from .env..."
        set -a
        source "$SCRIPT_DIR/.env"
        set +a
    else
        echo -e "${YELLOW}Warning: .env file not found${NC}"
        echo "Using system environment variables"
    fi
    
    # Verify requirements
    if [ -z "$GOOGLE_CLOUD_PROJECT" ]; then
        echo -e "${RED}Error: GOOGLE_CLOUD_PROJECT not set${NC}"
        return 1
    fi
    
    if [ -z "$GCP_GCS_BUCKET" ]; then
        echo -e "${RED}Error: GCP_GCS_BUCKET not set${NC}"
        return 1
    fi
    
    # Run the orchestrator
    python3 "$SCRIPT_DIR/bigquery_orchestrator.py" \
        --project-id "$GOOGLE_CLOUD_PROJECT" \
        --dataset-id "${BIGQUERY_DATASET_ID:-finance_data}" \
        --bucket-name "$GCP_GCS_BUCKET"
    
    return $?
}

# Main script logic
case "${1:-help}" in
    local)
        run_local_pipeline
        ;;
    setup)
        setup_local_environment
        ;;
    verify)
        verify_credentials && validate_csv_files
        ;;
    validate)
        validate_csv_files
        ;;
    help|--help|-h)
        print_usage
        ;;
    *)
        echo -e "${RED}Unknown option: $1${NC}"
        echo ""
        print_usage
        exit 1
        ;;
esac

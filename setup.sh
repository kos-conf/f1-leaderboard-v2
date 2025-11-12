#!/bin/bash
# Cross-platform setup script for F1 Leaderboard
# Supports macOS, Linux, and Windows (via Git Bash/WSL)

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Detect OS
detect_os() {
    case "$(uname -s)" in
        Linux*)     echo "linux";;
        Darwin*)    echo "macos";;
        CYGWIN*)    echo "windows";;
        MINGW*)     echo "windows";;
        MSYS*)      echo "windows";;
        *)          echo "unknown";;
    esac
}

OS=$(detect_os)

# Print colored messages
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
check_prerequisites() {
    print_info "Checking prerequisites..."
    
    local missing=0
    
    # Check Python
    if command_exists python3; then
        PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
        print_success "Python $PYTHON_VERSION found"
    else
        print_error "Python 3.11+ is required but not found"
        missing=1
    fi
    
    # Check pip
    if command_exists pip3; then
        print_success "pip3 found"
    else
        print_error "pip3 is required but not found"
        missing=1
    fi
    
    # Check Node.js
    if command_exists node; then
        NODE_VERSION=$(node --version)
        print_success "Node.js $NODE_VERSION found"
    else
        print_error "Node.js 18+ is required but not found"
        missing=1
    fi
    
    # Check npm
    if command_exists npm; then
        NPM_VERSION=$(npm --version)
        print_success "npm $NPM_VERSION found"
    else
        print_error "npm is required but not found"
        missing=1
    fi
    
    if [ $missing -eq 1 ]; then
        print_error "Please install missing prerequisites and try again"
        exit 1
    fi
    
    print_success "All prerequisites met"
}

# Setup backend
setup_backend() {
    print_info "Setting up backend..."
    
    cd backend || { print_error "Backend directory not found"; exit 1; }
    
    # Create virtual environment if it doesn't exist
    if [ ! -d "venv" ]; then
        print_info "Creating Python virtual environment..."
        python3 -m venv venv || { print_error "Failed to create virtual environment"; exit 1; }
    else
        print_info "Virtual environment already exists"
    fi
    
    # Activate virtual environment
    if [ "$OS" = "windows" ]; then
        source venv/Scripts/activate || { print_error "Failed to activate virtual environment"; exit 1; }
    else
        source venv/bin/activate || { print_error "Failed to activate virtual environment"; exit 1; }
    fi
    
    # Upgrade pip
    print_info "Upgrading pip..."
    pip install --upgrade pip --quiet || print_warning "Failed to upgrade pip, continuing..."
    
    # Install dependencies
    print_info "Installing backend dependencies..."
    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt || { print_error "Failed to install backend dependencies"; exit 1; }
        print_success "Backend dependencies installed"
    else
        print_warning "requirements.txt not found in backend directory"
    fi
    
    cd ..
    print_success "Backend setup complete"
}

# Setup admin
setup_admin() {
    print_info "Setting up admin..."
    
    cd admin || { print_error "Admin directory not found"; exit 1; }
    
    # Create virtual environment if it doesn't exist
    if [ ! -d "venv" ]; then
        print_info "Creating Python virtual environment..."
        python3 -m venv venv || { print_error "Failed to create virtual environment"; exit 1; }
    else
        print_info "Virtual environment already exists"
    fi
    
    # Activate virtual environment
    if [ "$OS" = "windows" ]; then
        source venv/Scripts/activate || { print_error "Failed to activate virtual environment"; exit 1; }
    else
        source venv/bin/activate || { print_error "Failed to activate virtual environment"; exit 1; }
    fi
    
    # Upgrade pip
    print_info "Upgrading pip..."
    pip install --upgrade pip --quiet || print_warning "Failed to upgrade pip, continuing..."
    
    # Install dependencies
    print_info "Installing admin dependencies..."
    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt || { print_error "Failed to install admin dependencies"; exit 1; }
        print_success "Admin dependencies installed"
    else
        print_warning "requirements.txt not found in admin directory"
    fi
    
    # Setup config.yaml if it doesn't exist
    if [ ! -f "config.yaml" ]; then
        if [ -f "config.yaml.example" ]; then
            print_info "Creating config.yaml from example..."
            cp config.yaml.example config.yaml
            print_warning "Please edit admin/config.yaml and add your Confluent Cloud API credentials"
            print_warning "You can get them from: https://confluent.cloud/settings/api-keys/create?tab=cloud"
        else
            print_warning "config.yaml.example not found, you'll need to create config.yaml manually"
        fi
    else
        print_info "config.yaml already exists"
    fi
    
    cd ..
    print_success "Admin setup complete"
}

# Setup frontend
setup_frontend() {
    print_info "Setting up frontend..."
    
    cd frontend || { print_error "Frontend directory not found"; exit 1; }
    
    # Install dependencies
    print_info "Installing frontend dependencies..."
    if [ -f "package.json" ]; then
        npm install || { print_error "Failed to install frontend dependencies"; exit 1; }
        print_success "Frontend dependencies installed"
    else
        print_warning "package.json not found in frontend directory"
    fi
    
    cd ..
    print_success "Frontend setup complete"
}

# Deploy infrastructure
deploy_infrastructure() {
    print_info "Deploying Confluent Cloud infrastructure..."
    
    cd admin || { print_error "Admin directory not found"; exit 1; }
    
    # Check if config.yaml exists
    if [ ! -f "config.yaml" ]; then
        print_error "admin/config.yaml not found"
        print_warning "Please create admin/config.yaml with your Confluent Cloud API credentials"
        print_warning "You can copy from config.yaml.example and add your credentials"
        cd ..
        return 1
    fi
    
    # Check if credentials are set (not placeholders)
    if ! grep -q "cloud_api_key:" config.yaml || ! grep -q "cloud_api_secret:" config.yaml; then
        print_warning "Confluent Cloud API credentials not found in config.yaml"
        print_warning "Please edit admin/config.yaml and add your credentials before deploying"
        cd ..
        return 1
    fi
    
    # Check if credentials are placeholders
    if grep -q "<YOUR_CONFLUENT_CLOUD_API_KEY>" config.yaml || grep -q "<YOUR_CONFLUENT_CLOUD_API_SECRET>" config.yaml; then
        print_warning "Confluent Cloud API credentials appear to be placeholders"
        print_warning "Please edit admin/config.yaml and replace placeholders with your actual credentials"
        cd ..
        return 1
    fi
    
    # Activate virtual environment
    if [ "$OS" = "windows" ]; then
        source venv/Scripts/activate || { print_error "Failed to activate virtual environment"; exit 1; }
    else
        source venv/bin/activate || { print_error "Failed to activate virtual environment"; exit 1; }
    fi
    
    # Run admin script
    print_info "Running infrastructure deployment script..."
    python main.py || { 
        print_error "Infrastructure deployment failed"
        cd ..
        return 1
    }
    
    cd ..
    print_success "Infrastructure deployment complete"
}

# Main execution
main() {
    echo ""
    echo "=========================================="
    echo "  F1 Leaderboard Setup Script"
    echo "=========================================="
    echo ""
    
    # Get script directory
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    cd "$SCRIPT_DIR"
    
    # Check prerequisites
    check_prerequisites
    echo ""
    
    # Setup backend
    setup_backend
    echo ""
    
    # Setup admin
    setup_admin
    echo ""
    
    # Setup frontend
    setup_frontend
    echo ""
    
    # Ask about infrastructure deployment
    echo ""
    if [ "$OS" = "windows" ]; then
        # Windows/Git Bash doesn't always support read -p well
        print_info "To deploy infrastructure, run: cd admin && source venv/bin/activate && python main.py"
        print_info "Make sure to configure admin/config.yaml with your Confluent Cloud API credentials first"
    else
        read -p "Do you want to deploy Confluent Cloud infrastructure now? (y/n) " -n 1 -r
        echo ""
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            deploy_infrastructure
        else
            print_info "Skipping infrastructure deployment"
            print_info "You can deploy later by running: cd admin && source venv/bin/activate && python main.py"
        fi
    fi
    
    echo ""
    echo "=========================================="
    print_success "Setup complete!"
    echo "=========================================="
    echo ""
    print_info "Next steps:"
    echo "  1. If you haven't deployed infrastructure, edit admin/config.yaml and run:"
    echo "     cd admin && source venv/bin/activate && python main.py"
    echo "  2. Follow README to proceed to Part 2"
    echo "  3. Start the backend: cd backend && source venv/bin/activate && python main.py"
    echo "  4. Start the frontend: cd frontend && npm run dev"
    echo ""
}

# Run main function
main "$@"


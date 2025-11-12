# Cross-platform setup script for F1 Leaderboard (PowerShell version for Windows)
# Run with: powershell -ExecutionPolicy Bypass -File setup.ps1

$ErrorActionPreference = "Stop"

# Colors for output
function Write-Info {
    param([string]$Message)
    Write-Host "ℹ️  $Message" -ForegroundColor Cyan
}

function Write-Success {
    param([string]$Message)
    Write-Host "✅ $Message" -ForegroundColor Green
}

function Write-Warning {
    param([string]$Message)
    Write-Host "⚠️  $Message" -ForegroundColor Yellow
}

function Write-Error {
    param([string]$Message)
    Write-Host "❌ $Message" -ForegroundColor Red
}

# Check if command exists
function Test-Command {
    param([string]$Command)
    $null = Get-Command $Command -ErrorAction SilentlyContinue
    return $?
}

# Check prerequisites
function Test-Prerequisites {
    Write-Info "Checking prerequisites..."
    
    $missing = $false
    
    # Check Python
    if (Test-Command "python") {
        $pythonVersion = python --version 2>&1
        Write-Success "Python $pythonVersion found"
    } elseif (Test-Command "python3") {
        $pythonVersion = python3 --version 2>&1
        Write-Success "Python $pythonVersion found"
    } else {
        Write-Error "Python 3.11+ is required but not found"
        $missing = $true
    }
    
    # Check pip
    if (Test-Command "pip") {
        Write-Success "pip found"
    } elseif (Test-Command "pip3") {
        Write-Success "pip3 found"
    } else {
        Write-Error "pip is required but not found"
        $missing = $true
    }
    
    # Check Node.js
    if (Test-Command "node") {
        $nodeVersion = node --version
        Write-Success "Node.js $nodeVersion found"
    } else {
        Write-Error "Node.js 18+ is required but not found"
        $missing = $true
    }
    
    # Check npm
    if (Test-Command "npm") {
        $npmVersion = npm --version
        Write-Success "npm $npmVersion found"
    } else {
        Write-Error "npm is required but not found"
        $missing = $true
    }
    
    if ($missing) {
        Write-Error "Please install missing prerequisites and try again"
        exit 1
    }
    
    Write-Success "All prerequisites met"
}

# Setup backend
function Setup-Backend {
    Write-Info "Setting up backend..."
    
    if (-not (Test-Path "backend")) {
        Write-Error "Backend directory not found"
        exit 1
    }
    
    Push-Location backend
    
    # Create virtual environment if it doesn't exist
    if (-not (Test-Path "venv")) {
        Write-Info "Creating Python virtual environment..."
        python -m venv venv
        if ($LASTEXITCODE -ne 0) {
            python3 -m venv venv
            if ($LASTEXITCODE -ne 0) {
                Write-Error "Failed to create virtual environment"
                Pop-Location
                exit 1
            }
        }
    } else {
        Write-Info "Virtual environment already exists"
    }
    
    # Activate virtual environment
    $activateScript = "venv\Scripts\Activate.ps1"
    if (Test-Path $activateScript) {
        & $activateScript
    } else {
        Write-Error "Failed to activate virtual environment"
        Pop-Location
        exit 1
    }
    
    # Upgrade pip
    Write-Info "Upgrading pip..."
    python -m pip install --upgrade pip --quiet
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Failed to upgrade pip, continuing..."
    }
    
    # Install dependencies
    Write-Info "Installing backend dependencies..."
    if (Test-Path "requirements.txt") {
        pip install -r requirements.txt
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Failed to install backend dependencies"
            Pop-Location
            exit 1
        }
        Write-Success "Backend dependencies installed"
    } else {
        Write-Warning "requirements.txt not found in backend directory"
    }
    
    Pop-Location
    Write-Success "Backend setup complete"
}

# Setup admin
function Setup-Admin {
    Write-Info "Setting up admin..."
    
    if (-not (Test-Path "admin")) {
        Write-Error "Admin directory not found"
        exit 1
    }
    
    Push-Location admin
    
    # Create virtual environment if it doesn't exist
    if (-not (Test-Path "venv")) {
        Write-Info "Creating Python virtual environment..."
        python -m venv venv
        if ($LASTEXITCODE -ne 0) {
            python3 -m venv venv
            if ($LASTEXITCODE -ne 0) {
                Write-Error "Failed to create virtual environment"
                Pop-Location
                exit 1
            }
        }
    } else {
        Write-Info "Virtual environment already exists"
    }
    
    # Activate virtual environment
    $activateScript = "venv\Scripts\Activate.ps1"
    if (Test-Path $activateScript) {
        & $activateScript
    } else {
        Write-Error "Failed to activate virtual environment"
        Pop-Location
        exit 1
    }
    
    # Upgrade pip
    Write-Info "Upgrading pip..."
    python -m pip install --upgrade pip --quiet
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Failed to upgrade pip, continuing..."
    }
    
    # Install dependencies
    Write-Info "Installing admin dependencies..."
    if (Test-Path "requirements.txt") {
        pip install -r requirements.txt
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Failed to install admin dependencies"
            Pop-Location
            exit 1
        }
        Write-Success "Admin dependencies installed"
    } else {
        Write-Warning "requirements.txt not found in admin directory"
    }
    
    # Setup config.yaml if it doesn't exist
    if (-not (Test-Path "config.yaml")) {
        if (Test-Path "config.yaml.example") {
            Write-Info "Creating config.yaml from example..."
            Copy-Item "config.yaml.example" "config.yaml"
            Write-Warning "Please edit admin/config.yaml and add your Confluent Cloud API credentials"
            Write-Warning "You can get them from: https://confluent.cloud/settings/api-keys/create?tab=cloud"
        } else {
            Write-Warning "config.yaml.example not found, you'll need to create config.yaml manually"
        }
    } else {
        Write-Info "config.yaml already exists"
    }
    
    Pop-Location
    Write-Success "Admin setup complete"
}

# Setup frontend
function Setup-Frontend {
    Write-Info "Setting up frontend..."
    
    if (-not (Test-Path "frontend")) {
        Write-Error "Frontend directory not found"
        exit 1
    }
    
    Push-Location frontend
    
    # Install dependencies
    Write-Info "Installing frontend dependencies..."
    if (Test-Path "package.json") {
        npm install
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Failed to install frontend dependencies"
            Pop-Location
            exit 1
        }
        Write-Success "Frontend dependencies installed"
    } else {
        Write-Warning "package.json not found in frontend directory"
    }
    
    Pop-Location
    Write-Success "Frontend setup complete"
}

# Deploy infrastructure
function Deploy-Infrastructure {
    Write-Info "Deploying Confluent Cloud infrastructure..."
    
    if (-not (Test-Path "admin")) {
        Write-Error "Admin directory not found"
        return $false
    }
    
    Push-Location admin
    
    # Check if config.yaml exists
    if (-not (Test-Path "config.yaml")) {
        Write-Error "admin/config.yaml not found"
        Write-Warning "Please create admin/config.yaml with your Confluent Cloud API credentials"
        Write-Warning "You can copy from config.yaml.example and add your credentials"
        Pop-Location
        return $false
    }
    
    # Check if credentials are set
    $configContent = Get-Content "config.yaml" -Raw
    if ($configContent -notmatch "cloud_api_key:" -or $configContent -notmatch "cloud_api_secret:") {
        Write-Warning "Confluent Cloud API credentials not found in config.yaml"
        Write-Warning "Please edit admin/config.yaml and add your credentials before deploying"
        Pop-Location
        return $false
    }
    
    # Check if credentials are placeholders
    if ($configContent -match "<YOUR_CONFLUENT_CLOUD_API_KEY>" -or $configContent -match "<YOUR_CONFLUENT_CLOUD_API_SECRET>") {
        Write-Warning "Confluent Cloud API credentials appear to be placeholders"
        Write-Warning "Please edit admin/config.yaml and replace placeholders with your actual credentials"
        Pop-Location
        return $false
    }
    
    # Activate virtual environment
    $activateScript = "venv\Scripts\Activate.ps1"
    if (Test-Path $activateScript) {
        & $activateScript
    } else {
        Write-Error "Failed to activate virtual environment"
        Pop-Location
        return $false
    }
    
    # Run admin script
    Write-Info "Running infrastructure deployment script..."
    python main.py
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Infrastructure deployment failed"
        Pop-Location
        return $false
    }
    
    Pop-Location
    Write-Success "Infrastructure deployment complete"
    return $true
}

# Main execution
function Main {
    Write-Host ""
    Write-Host "=========================================="
    Write-Host "  F1 Leaderboard Setup Script"
    Write-Host "=========================================="
    Write-Host ""
    
    # Get script directory
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    Set-Location $scriptDir
    
    # Check prerequisites
    Test-Prerequisites
    Write-Host ""
    
    # Setup backend
    Setup-Backend
    Write-Host ""
    
    # Setup admin
    Setup-Admin
    Write-Host ""
    
    # Setup frontend
    Setup-Frontend
    Write-Host ""
    
    # Ask about infrastructure deployment
    Write-Host ""
    $response = Read-Host "Do you want to deploy Confluent Cloud infrastructure now? (y/n)"
    if ($response -eq "y" -or $response -eq "Y") {
        Deploy-Infrastructure
    } else {
        Write-Info "Skipping infrastructure deployment"
        Write-Info "You can deploy later by running: cd admin; .\venv\Scripts\Activate.ps1; python main.py"
    }
    
    Write-Host ""
    Write-Host "=========================================="
    Write-Success "Setup complete!"
    Write-Host "=========================================="
    Write-Host ""
    Write-Info "Next steps:"
    Write-Host "  1. If you haven't deployed infrastructure, edit admin/config.yaml and run:"
    Write-Host "     cd admin; .\venv\Scripts\Activate.ps1; python main.py"
    Write-Host "  2. Follow README to proceed to Part 2"
    Write-Host "  3. Start the backend: cd backend; .\venv\Scripts\Activate.ps1; python main.py"
    Write-Host "  3. Start the frontend: cd frontend; npm run dev"
    Write-Host ""
}

# Run main function
Main


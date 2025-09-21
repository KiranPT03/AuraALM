# AuraALM Authentication Service

A FastAPI-based authentication microservice for the AuraALM (Application Lifecycle Management) platform. This service provides comprehensive user authentication, registration, and authorization capabilities with JWT token management and MongoDB integration.

## 🚀 Features

- **User Registration & Authentication**: Complete user registration with hierarchical data structure
- **JWT Token Management**: Access and refresh token handling with configurable expiration
- **Password Security**: BCrypt hashing with configurable salt rounds
- **MongoDB Integration**: Flexible NoSQL database integration for user data storage
- **CORS Support**: Configurable Cross-Origin Resource Sharing
- **Comprehensive Logging**: Structured logging with file and console handlers
- **API Documentation**: Auto-generated OpenAPI/Swagger documentation
- **User Profile Management**: Hierarchical user data with profiles, preferences, security settings
- **Role-Based Access**: User roles and group management
- **Session Management**: Login/logout tracking and session timeout

## 📋 Prerequisites

- Python 3.8+
- MongoDB 4.0+
- pip (Python package manager)

## 🛠️ Installation

### 1. Clone the Repository
```bash
git clone <repository-url>
cd AuraALM/Services/AuthService
```

### 2. Create Virtual Environment
```bash
# Windows
python -m venv .venv
.venv\Scripts\activate

# Linux/macOS
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure MongoDB
Ensure MongoDB is running on your system:
```bash
# Start MongoDB service
# Windows (if installed as service)
net start MongoDB

# Linux/macOS
sudo systemctl start mongod
# or
brew services start mongodb-community
```

### 5. Configuration Setup
Update the configuration in `scripts/config/application.yaml`:

```yaml
database:
  mongodb:
    host: "localhost"
    port: 27017
    database: "automator_db"
    username: "user_automator"
    password: "p@ssw0rd@Automator"
    # ... other MongoDB settings
```

### 6. Run the Application
```bash
python main.py
```

The service will start on `http://localhost:5000` by default.

## 📁 Project Structure
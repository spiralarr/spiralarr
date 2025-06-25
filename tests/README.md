# Tests for Spiralarr

This directory contains the test suite for spiralarr, organized into proper test modules.

## Test Structure

### 📁 **Test Files**

- **`conftest.py`** - Pytest configuration and shared fixtures
- **`test_models.py`** - Unit tests for database models
- **`test_operators.py`** - Unit tests for operators (Bash, Python)
- **`test_integration.py`** - Integration tests for Phase 1 functionality
- **`test_phase1.py`** - Comprehensive standalone integration test

### 🧪 **Test Categories**

#### Unit Tests
- **Models**: Test database model creation, relationships, and state management
- **Operators**: Test operator execution, error handling, and parameter passing

#### Integration Tests
- **Complete Flow**: Test end-to-end task execution from database to completion
- **Error Handling**: Test failure scenarios and state management
- **Phase 1 Verification**: Comprehensive test of all Phase 1 components

## Running Tests

### Run All Tests
```bash
uv run --with pytest pytest tests/ -v
```

### Run Specific Test Categories
```bash
# Unit tests only
uv run --with pytest pytest tests/test_models.py tests/test_operators.py -v

# Integration tests only
uv run --with pytest pytest tests/test_integration.py -v

# Standalone Phase 1 test
uv run python tests/test_phase1.py
```

### Run with Coverage
```bash
uv run --with pytest pytest tests/ --cov=src/spiralarr --cov-report=html
```

## Test Results ✅

All tests are currently passing:
- **10 pytest tests** - Unit and integration tests
- **1 standalone test** - Comprehensive Phase 1 verification
- **Clean test isolation** - Each test uses temporary databases
- **Proper fixtures** - Shared test configuration and data

## Test Features

### 🔧 **Fixtures**
- `temp_db` - Temporary SQLite database for each test
- `sample_dag_config` - Sample DAG configuration for testing

### 🎯 **Coverage**
- Database models and relationships
- Operator execution and error handling
- Task runner functionality
- State management and transitions
- Integration between components

### 🚀 **Benefits**
- **Fast execution** - All tests run in under 1 second
- **Isolated** - No test interference with temporary databases
- **Comprehensive** - Covers all Phase 1 functionality
- **Maintainable** - Clear structure and good separation of concerns

#!/bin/bash
# Run tests

echo "Running unit tests..."
pytest tests/ -v --tb=short

echo ""
echo "Running with coverage..."
pytest tests/ --cov=src --cov-report=html

echo ""
echo "Coverage report generated in htmlcov/index.html"

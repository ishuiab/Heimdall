#!/bin/bash

# Bot Manager API Test Script
# Tests all the new API endpoints

BASE_URL="http://localhost:5000"

echo "=========================================="
echo "Bot Manager API Test Suite"
echo "=========================================="
echo ""

# Test 1: Get strategies
echo "Test 1: GET /api/bot/strategies"
echo "Command: curl -s $BASE_URL/api/bot/strategies | python -m json.tool"
echo ""

# Test 2: Get brokers for SHOONYA
echo "Test 2: GET /api/bot/brokers?strategy=SHOONYA"
echo "Command: curl -s '$BASE_URL/api/bot/brokers?strategy=SHOONYA' | python -m json.tool"
echo ""

# Test 3: Get accounts for SHOONYA/FA394567
echo "Test 3: GET /api/bot/accounts?strategy=SHOONYA&broker=FA394567"
echo "Command: curl -s '$BASE_URL/api/bot/accounts?strategy=SHOONYA&broker=FA394567' | python -m json.tool"
echo ""

# Test 4: Get configs for SHOONYA/FA394567/ETF_FMV
echo "Test 4: GET /api/bot/configs?strategy=SHOONYA&broker=FA394567&account=ETF_FMV"
echo "Command: curl -s '$BASE_URL/api/bot/configs?strategy=SHOONYA&broker=FA394567&account=ETF_FMV' | python -m json.tool"
echo ""

echo "=========================================="
echo "To run these tests, execute:"
echo "  chmod +x test_bot_manager_api.sh"
echo "  ./test_bot_manager_api.sh"
echo ""
echo "Make sure the Flask server is running on port 5000"
echo "=========================================="

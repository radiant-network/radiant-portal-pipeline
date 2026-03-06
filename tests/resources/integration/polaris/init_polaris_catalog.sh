#!/bin/bash

# Get token
echo "get token"
token_response=$(curl --fail-with-body -s -S -X POST "http://$POLARIS_SERVICE_HOST/api/catalog/v1/oauth/tokens" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d "grant_type=client_credentials&client_id=${REALM_USER}&client_secret=${REALM_PASSWORD}&scope=PRINCIPAL_ROLE:ALL" 2>&1)
token=$(echo "$token_response" | grep -o '"access_token":"[^"]*"' | sed 's/"access_token":"//;s/"//')

# Creating catalog
catalog_payload="{
  \"catalog\": {
    \"name\": \"${CATALOG_NAME}\",
    \"type\": \"INTERNAL\",
    \"readOnly\": false,
    \"properties\": {
      \"default-base-location\": \"${CATALOG_WAREHOUSE}\"
    },
    \"storageConfigInfo\": {
      \"storageType\": \"S3\",
      \"allowedLocations\": [\"${CATALOG_WAREHOUSE}\"],
      \"pathStyleAccess\": true
    }
  }
}"
echo "creating catalog"
curl --fail-with-body -s -S -X POST "http://$POLARIS_SERVICE_HOST/api/management/v1/catalogs" \
  -H "Authorization: Bearer $token" -H "Polaris-Realm: $REALM_NAME" -H "Accept: application/json" -H "Content-Type: application/json" \
  -d "$catalog_payload"
echo "\n"

# Granting privilege CATALOG_MANAGE_CONTENT to realm root user
grant_payload="{\"type\": \"catalog\", \"privilege\": \"CATALOG_MANAGE_CONTENT\"}"
echo "granting privilege"
curl --fail-with-body -s -S -X PUT "http://$POLARIS_SERVICE_HOST/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles/catalog_admin/grants" \
  -H "Authorization: Bearer $token" -H "Polaris-Realm: $REALM_NAME" -H "Accept: application/json" -H "Content-Type: application/json" \
  -d "$grant_payload"
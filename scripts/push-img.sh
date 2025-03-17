#!/bin/bash

# Set variables

IMAGE_NAME="pricingdev"
ACR_NAME="pricingpoc"
DEVCONTAINER_DIR=".devcontainer"
ACR_REPO="$ACR_NAME.azurecr.io/$IMAGE_NAME"
HASH_FILE="$DEVCONTAINER_DIR/.devcontainer-hash.txt"

echo "Logging into Azure Container Registry..."
az acr login --name "$ACR_NAME"

HASH=$(cat "$HASH_FILE" | tr -d '[:space:]')

# Push the image to Azure Container Registry
echo "Pushing image to ACR..."
docker push "${ACR_REPO}:${HASH}"

echo "Image pushed successfully: ${ACR_REPO}:${HASH}"
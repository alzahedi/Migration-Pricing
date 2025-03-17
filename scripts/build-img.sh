#!/bin/bash

# Set variables
DEVCONTAINER_DIR=".devcontainer"
HASH_FILE="$DEVCONTAINER_DIR/.devcontainer-hash.txt"
IMAGE_NAME="pricingdev"
ACR_NAME="pricingpoc"
ACR_REPO="$ACR_NAME.azurecr.io/$IMAGE_NAME"

# Compute the hash of all files in .devcontainer/
HASH=$(find "$DEVCONTAINER_DIR" -type f -exec sha256sum {} + | sha256sum | awk '{print $1}')

# Store the hash in a file
echo "$HASH" > "$HASH_FILE"
echo "Computed hash: $HASH"

# Build the Dev Container using the computed hash as the image tag
CMD=(
    devcontainer build
    --workspace-folder .
    --config "$DEVCONTAINER_DIR/devcontainer.local.json"
    --image-name "${ACR_REPO}:${HASH}"
)

# Print and execute the command
echo "Running: ${CMD[*]}"
"${CMD[@]}"

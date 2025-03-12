#!/bin/bash
echo "Container started."

# Check if the user is already logged into Azure
if ! az account show &>/dev/null; then
    echo "You are not logged into Azure. Please log in using the device code flow."
    az login --use-device
else
    echo "Azure CLI login detected."
fi

# Start an interactive shell
exec "$@"

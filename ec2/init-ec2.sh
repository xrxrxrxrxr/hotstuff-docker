#!/bin/bash
# init-ec2.sh - Install Docker and Docker Compose

SSH_KEY=~/.ssh/xrui.pem
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HOSTS_FILE="$SCRIPT_DIR/hosts.txt"

if [[ ! -f "$HOSTS_FILE" ]]; then
  echo "hosts.txt not found at $HOSTS_FILE" >&2
  exit 1
fi

mapfile -t NODES < <(awk '/node[0-9]+$/ {print "ubuntu@"$1}' "$HOSTS_FILE")

if [[ ${#NODES[@]} -eq 0 ]]; then
  echo "No node entries found in hosts.txt" >&2
  exit 1
fi

echo "Initializing EC2 instances (installing Docker)..."

for node in "${NODES[@]}"; do
  echo "Initializing $node..."
  ssh $SSH_OPTS $node "
    set -e
    # Update the system
    sudo apt-get update -y

    # Install Docker
    sudo apt-get install -y docker.io

    # Start Docker
    sudo systemctl start docker
    sudo systemctl enable docker

    # Add the current user to the docker group
    sudo usermod -aG docker ubuntu

    # Remove legacy docker-compose (if present)
    sudo rm -f /usr/local/bin/docker-compose

    # Install dependencies
    sudo apt-get install -y ca-certificates curl gnupg lsb-release

    # Add the official Docker GPG key
    sudo mkdir -p /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

    # Add the official Docker apt repository
    echo \"deb [arch=\$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \$(lsb_release -cs) stable\" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

    # Update apt and install the docker compose plugin
    sudo apt-get update -y
    sudo apt-get install -y docker-compose-plugin

    # Verify the installation
    docker --version
    docker compose version
  " &
done

wait
echo ""
echo "All instances initialized!"
echo "Note: log in again to refresh docker group permissions"
echo ""
echo "Next steps:"
echo "  make deploy   # Deploy configuration files"
echo "  make start    # Start the experiment"

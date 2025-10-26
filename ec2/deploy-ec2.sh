#!/usr/bin/env bash
# deploy-ec2.sh

SSH_KEY=~/.ssh/xrui.pem
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

# Read hosts.txt
declare -A PUBLIC_IPS PRIVATE_IPS

echo "Reading hosts.txt..."
while IFS=' ' read -r ip name; do
  # Skip comments and empty lines
  [[ $ip =~ ^#.*$ || -z $ip ]] && continue
  
  if [[ ! $name =~ -private$ ]]; then
    PUBLIC_IPS[$name]=$ip
    echo "  ✓ $name = $ip"
  else
    base_name=${name%-private}
    PRIVATE_IPS[$base_name]=$ip
  fi
done < hosts.txt

echo ""

# Verify we loaded the IP list
if [[ ${#PUBLIC_IPS[@]} -eq 0 ]]; then
  echo "Error: failed to read any IP from hosts.txt"
  echo "Please check the format of hosts.txt"
  exit 1
fi

echo "Starting deployment to EC2..."
echo ""

# Deploy nodes 0-3 (in parallel)
declare -a DEPLOY_PIDS
STATUS=0

for i in {0..3}; do
  (
    ip="${PUBLIC_IPS[node$i]}"
    if [[ -z "$ip" ]]; then
      echo "Error: IP for node$i not found"
      exit 1
    fi

    echo "Deploying node$i ($ip)..."

    set -e
    ssh $SSH_OPTS ubuntu@$ip "mkdir -p ~/hotstuff/logs"
    scp $SSH_OPTS docker-compose-node.yml ubuntu@$ip:~/hotstuff/docker-compose.yml
    scp $SSH_OPTS envs/node$i.env ubuntu@$ip:~/hotstuff/.env

    echo "node$i deployment completed"
    echo ""
  ) &
  DEPLOY_PIDS[$i]=$!
done

for i in {0..3}; do
  if ! wait "${DEPLOY_PIDS[$i]}"; then
    echo "Error during node$i deployment"
    STATUS=1
  fi
done

if [[ $STATUS -ne 0 ]]; then
  echo "Some nodes failed to deploy, aborting"
  exit 1
fi

# Deploy the client
client_ip="${PUBLIC_IPS[client]}"
if [[ -z "$client_ip" ]]; then
  echo "Error: client IP not found"
  exit 1
fi

echo "Deploying client ($client_ip)..."
ssh $SSH_OPTS ubuntu@$client_ip "mkdir -p ~/hotstuff/logs"
scp $SSH_OPTS docker-compose-client.yml ubuntu@$client_ip:~/hotstuff/docker-compose.yml
scp $SSH_OPTS envs/client.env ubuntu@$client_ip:~/hotstuff/.env

echo ""
echo "All files deployed successfully!"

# EC2 distributed deployment Makefile
SSH_KEY = ~/.ssh/xrui.pem


# Parse node0~3 public IPs from hosts.txt automatically
NODES := $(shell grep -E 'node[0-9]+$$' hosts.txt | awk '{print "ubuntu@"$$1}')
NODE0 := $(shell awk '$$2=="node0" {print "ubuntu@"$$1}' hosts.txt)
# Parse the client public IP from hosts.txt
CLIENT := $(shell grep -E 'client$$' hosts.txt | awk '{print "ubuntu@"$$1}')
# Parse all public IPs from hosts.txt
HOST_IPS := $(shell grep -v private hosts.txt | awk '{print $$1}')

SSH_OPTS = -i $(SSH_KEY) -o StrictHostKeyChecking=accept-new -o BatchMode=yes -o ConnectTimeout=8


.PHONY: init deploy start stop logs known_hosts ping hosts-clean clean-ec2 clean

known_hosts:
	@echo "Initializing known_hosts..."
	@ssh-keyscan -H $(HOST_IPS) >> $$HOME/.ssh/known_hosts 2>/dev/null || true
	@echo "known_hosts updated"

ping:
	@for host in $(NODES) $(CLIENT); do \
	  echo "↪︎ Testing $$host"; \
	  ssh $(SSH_OPTS) $$host 'echo OK' || echo "$$host connection failed"; \
	done

init:
	@bash init-ec2.sh
	@$(MAKE) pull

deploy:
	@/opt/homebrew/bin/bash deploy-ec2.sh


pull:
	@echo "Pulling node and client images..."
	@status=0; pids=""; \
	for node in $(NODES); do \
		( \
			echo "Pulling hotstuff-node image on $${node}..."; \
			if ! ssh $(SSH_OPTS) $${node} "cd ~/hotstuff && docker compose pull hotstuff-node"; then \
				echo "Warning: failed to pull on $${node}"; \
				exit 1; \
			fi \
		) & \
		pids="$$pids $$!"; \
	done; \
	for pid in $$pids; do \
		if ! wait $$pid; then status=1; fi; \
	done; \
	if [ $$status -ne 0 ]; then \
		echo "Warning: one or more node pulls failed"; \
	fi
	@echo "Pulling client image ($(CLIENT)) ..."
	@ssh $(SSH_OPTS) $(CLIENT) "cd ~/hotstuff && docker compose pull hotstuff-client" || true
	@echo "Image pull complete"

start: 
	@$(MAKE) hosts-clean
	@echo "Starting the experiment..."
	@status=0; pids=""; \
	for node in $(NODES); do \
		( \
			ssh $(SSH_OPTS) $${node} "cd ~/hotstuff && docker compose --profile consensus up -d" \
		) & \
		pids="$$pids $$!"; \
	done; \
	for pid in $$pids; do \
		if ! wait $$pid; then status=1; fi; \
	done; \
		if [ $$status -ne 0 ]; then \
			echo "Failed to start consensus nodes"; \
		exit 1; \
	fi
	@sleep 20
	@ssh $(SSH_OPTS) $(CLIENT) "cd ~/hotstuff && docker compose --profile client up -d || docker-compose --profile client up -d" || { echo "Failed to start client"; exit 1; }
	@echo "Experiment startup complete"
	@echo "Waiting 2 minutes before automatically stopping..."
	@sleep 150
	@$(MAKE) stop


stop:
	@echo "Stopping the experiment..."
	@status=0; pids=""; \
	for node in $(NODES); do \
		echo "→ $$node"; \
		( \
			ssh $(SSH_OPTS) $$node "cd ~/hotstuff && (docker compose --profile consensus down || docker-compose --profile consensus down)" \
		) & \
		pids="$$pids $$!"; \
	done; \
	for pid in $$pids; do \
		if ! wait $$pid; then status=1; fi; \
	done; \
		if [ $$status -ne 0 ]; then \
			echo "Failed to stop consensus nodes"; \
		exit 1; \
	fi
	@ssh $(SSH_OPTS) $(CLIENT) "cd ~/hotstuff && (docker compose --profile client down || docker-compose --profile client down)" || { echo "Failed to stop client"; exit 1; }
	@echo "Experiment stopped; preparing to collect logs"
	@$(MAKE) logs-core

logs:
	@echo "Collecting logs..."
	@mkdir -p ec2-logs
	@status=0; pids=""; \
	for node in $(NODES); do \
		host=$${node##*@}; \
		( scp $(SSH_OPTS) -r $$node:~/hotstuff/logs/* ./ec2-logs/logs-$$host ) & \
		pids="$$pids $$!"; \
	done; \
	( scp $(SSH_OPTS) -r $(CLIENT):~/hotstuff/logs ./ec2-logs/logs-client ) & \
	pids="$$pids $$!"; \
	for pid in $$pids; do \
		if ! wait $$pid; then status=1; fi; \
	done; \
	if [ $$status -ne 0 ]; then \
		echo "Failed to collect logs"; \
		exit 1; \
	fi
	@echo "Log collection complete"
	@$(MAKE) clean

logs-core:
	@echo "Collecting node0 and client logs..."
	@mkdir -p ec2-logs
	@status=0; pids=""; \
	node="$(NODE0)"; \
	if [ -n "$$node" ]; then \
		host=$${node##*@}; \
		( scp $(SSH_OPTS) -r $$node:~/hotstuff/logs/* ./ec2-logs/logs-$$host ) & \
		pids="$$pids $$!"; \
	else \
		echo "Warning: node0 public IP not found, skipping node log collection"; \
	fi; \
	( scp $(SSH_OPTS) -r $(CLIENT):~/hotstuff/logs ./ec2-logs/logs-client ) & \
	pids="$$pids $$!"; \
	for pid in $$pids; do \
		if ! wait $$pid; then status=1; fi; \
	done; \
	if [ $$status -ne 0 ]; then \
		echo "Failed to collect core logs"; \
		exit 1; \
	fi
	@echo "Core log collection complete"
	@$(MAKE) clean

hosts-clean:
	@echo "Cleaning old known_hosts entries (EC2 IPs only)..."
	@for ip in $$(awk '{print $$1}' hosts.txt | grep -E '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$$'); do \
		ssh-keygen -R $$ip >/dev/null 2>&1; \
	done
	@echo "EC2 IP cleanup done"

clean:
	@echo "Cleaning log directories..."
	@for ip in $$(awk '{if ($$1 !~ /^172\./ && ($$2 ~ /^node/ || $$2 ~ /^client/)) print $$1}' hosts.txt); do \
		( \
			echo "→ Cleaning $$ip ..."; \
			if ssh $(SSH_OPTS) ubuntu@$$ip "sudo rm -rf ~/hotstuff/logs/*"; then \
				echo "$$ip cleanup complete"; \
			else \
				echo "Warning: $$ip cleanup failed"; \
			fi \
		) & \
	done; \
	wait
	@echo "Cleanup finished"
	@$(MAKE) hosts-clean

clean-ec2:
	@echo "Cleaning EC2 node data (public IPs only)..."
	@for ip in $$(awk '{if ($$1 !~ /^172\./ && ($$2 ~ /^node/ || $$2 ~ /^client/)) print $$1}' hosts.txt); do \
		echo "→ Cleaning $$ip ..."; \
		ssh $(SSH_OPTS) ubuntu@$$ip "sudo rm -rf ~/hotstuff" || echo "Warning: $$ip cleanup failed"; \
	done
	@echo "Cleanup finished"

clean-images:
	@echo "Cleaning old images on EC2 nodes..."
	@for node in $(NODES) $(CLIENT); do \
		( \
			echo "→ $$node"; \
			if ssh $(SSH_OPTS) $$node "docker system prune -a -f --volumes"; then \
				echo "$$node image cleanup complete"; \
			else \
				echo "Warning: $$node image cleanup failed"; \
			fi \
		) & \
	done; \
	wait
	@echo "Image cleanup complete"

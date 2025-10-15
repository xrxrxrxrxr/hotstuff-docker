# EC2 分布式部署 Makefile
SSH_KEY = ~/.ssh/xrui.pem


# 从 hosts.txt 自动解析出 node0~3 的公网 IP 列表
NODES := $(shell grep -E 'node[0-9]+$$' hosts.txt | awk '{print "ubuntu@"$$1}')
# 从 hosts.txt 自动解析出 client 的公网 IP
CLIENT := $(shell grep -E 'client$$' hosts.txt | awk '{print "ubuntu@"$$1}')
# 从 hosts.txt 自动解析出所有公网 IP
HOST_IPS := $(shell grep -v private hosts.txt | awk '{print $$1}')

SSH_OPTS = -i $(SSH_KEY) -o StrictHostKeyChecking=accept-new -o BatchMode=yes -o ConnectTimeout=8


.PHONY: init deploy start stop logs known_hosts ping hosts-clean clean-ec2 clean

known_hosts:
	@echo "🔐 初始化 known_hosts..."
	@ssh-keyscan -H $(HOST_IPS) >> $$HOME/.ssh/known_hosts 2>/dev/null || true
	@echo "✅ known_hosts 已更新"

ping:
	@for host in $(NODES) $(CLIENT); do \
	  echo "↪︎ 测试 $$host"; \
	  ssh $(SSH_OPTS) $$host 'echo OK' || echo "❌ $$host 连接失败"; \
	done

init:
	@bash init-ec2.sh

deploy:
	@/opt/homebrew/bin/bash deploy-ec2.sh


pull:
	@echo "🚀 拉取节点与客户端镜像..."
	@for node in $(NODES); do \
		echo "🔸 拉取 $${node} 上的 hotstuff-node 镜像..."; \
		ssh $(SSH_OPTS) $${node} "cd ~/hotstuff && docker compose pull hotstuff-node" || true; \
	done; \
	wait
	@echo "🔸 拉取客户端镜像 ($(CLIENT)) ..."
	@ssh $(SSH_OPTS) $(CLIENT) "cd ~/hotstuff && docker compose pull hotstuff-client" || true
	@echo "✅ 镜像拉取完成 ✅"

start: 
	@echo "🚀 启动实验..."
	@for node in $(NODES); do \
		ssh $(SSH_OPTS) $${node} "cd ~/hotstuff && docker compose --profile consensus up -d" & \
	done; \
	wait
	@sleep 15
	@ssh $(SSH_OPTS) $(CLIENT) "cd ~/hotstuff && docker compose --profile client up -d || docker-compose --profile client up -d"
	@echo "✅ 实验启动完成"
	@echo "⏱️ 等待 2 分钟后自动停止实验..."
	@sleep 150
	@$(MAKE) stop

stop:
	@echo "🛑 停止实验..."
	@for node in $(NODES); do \
		echo "→ $$node"; \
		ssh $(SSH_OPTS) $$node "cd ~/hotstuff && (docker compose --profile consensus down || docker-compose --profile consensus down)" & \
	done; \
	wait
	@ssh $(SSH_OPTS) $(CLIENT) "cd ~/hotstuff && (docker compose --profile client down || docker-compose --profile client down)"
	@echo "✅ 实验停止完成, 准备收集日志"
	@$(MAKE) logs

logs:
	@echo "📦 收集日志..."
	@mkdir -p ec2-logs
	@for node in $(NODES); do \
		host=$${node##*@}; \
		scp $(SSH_OPTS) -r $$node:~/hotstuff/logs/* ./ec2-logs/logs-$$host & \
	done; \
	scp $(SSH_OPTS) -r $(CLIENT):~/hotstuff/logs ./ec2-logs/logs-client & \
	wait
	@echo "✅ 日志收集完成"
	@$(MAKE) clean

hosts-clean:
	@echo "🔐 清理旧 known_hosts 记录（仅限 EC2 IP）..."
	@for ip in $$(awk '{print $$1}' hosts.txt | grep -E '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$$'); do \
		ssh-keygen -R $$ip >/dev/null 2>&1; \
	done
	@echo "✅ EC2 IP 已清理完毕"

clean:
	@echo "🧹 开始清理 log..."
	@for ip in $$(awk '{if ($$1 !~ /^172\./ && ($$2 ~ /^node/ || $$2 ~ /^client/)) print $$1}' hosts.txt); do \
		echo "→ 清理 $$ip ..."; \
		ssh $(SSH_OPTS) ubuntu@$$ip "sudo rm -rf ~/hotstuff/logs/*" || echo "⚠️ $$ip 清理失败"; \
	done
	@echo "✅ 清理完成"

clean-ec2:
	@echo "🧹 开始清理 EC2 节点数据（仅公网IP）..."
	@for ip in $$(awk '{if ($$1 !~ /^172\./ && ($$2 ~ /^node/ || $$2 ~ /^client/)) print $$1}' hosts.txt); do \
		echo "→ 清理 $$ip ..."; \
		ssh $(SSH_OPTS) ubuntu@$$ip "sudo rm -rf ~/hotstuff" || echo "⚠️ $$ip 清理失败"; \
	done
	@echo "✅ 清理完成"

clean-images:
	@echo "🧼 清理 EC2 节点上的旧镜像..."
	@for node in $(NODES) $(CLIENT); do \
		echo "→ $$node"; \
		ssh $(SSH_OPTS) $$node "docker system prune -a -f --volumes"; \
	done
	@echo "✅ 镜像清理完成"
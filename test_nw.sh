#!/bin/bash
# test_network_communication.sh

echo "🌐 测试Pompe网络通信"



echo "1. 检查网络对象创建:"
docker-compose logs | grep "创建网络支持的Pompe管理器" | tail -3

echo -e "\n2. 检查服务器启动:"
docker-compose logs | grep "Pompe服务器监听" | tail -4

echo -e "\n3. 检查网络广播准备:"
docker-compose logs | grep "准备通过网络广播" | tail -3

echo -e "\n4. 检查广播结果:"
docker-compose logs | grep "Ordering1广播" | tail -5

echo -e "\n5. 检查连接尝试:"
docker-compose logs | grep "Pompe发送到节点" | tail -5

echo -e "\n6. 检查消息接收:"
docker-compose logs | grep "消息接收.*收到来自节点" | tail -5

echo -e "\n7. 检查端口监听:"
docker-compose exec node0 netstat -tlnp | grep 20000

echo -e "\n8. 测试连接:"
docker-compose exec node0 nc -zv node1 20001
docker-compose exec node0 nc -zv node2 20002
docker-compose exec node0 nc -zv node3 20003

echo -e "\n9. 实时监控网络流程:"
docker-compose logs -f | grep -E "(网络广播|Pompe发送|消息接收|网络处理)"
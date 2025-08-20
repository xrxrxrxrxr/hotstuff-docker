#!/bin/bash
# diagnose_pompe.sh - 深度诊断Pompe流水线

echo "🔍 Pompe 流水线深度诊断"

echo "1. 检查交易输入:"
docker-compose logs | grep "接收.*交易" | tail -5


echo -e "\n3. 检查Ordering1阶段:"
docker-compose logs | grep "Ordering1.*处理" | tail -5

echo -e "\n4. 检查时间戳收集:"
docker-compose logs | grep "收集进度" | tail -5

echo -e "\n5. 检查Ordering2阶段:"
docker-compose logs | grep "Ordering2.*处理" | tail -5

echo -e "\n6. 检查提交集状态:"
docker-compose logs | grep "提交集" | tail -5

echo -e "\n8. 检查输出到HotStuff:"
docker-compose logs | grep "出到HotStuff" | tail -5

echo -e "\n9. 检查HotStuff队列:"
docker-compose logs | grep "队列状态" | tail -5

echo -e "\n10. 纯共识TPS:"
docker-compose logs | grep "纯共识TPS" | tail -3
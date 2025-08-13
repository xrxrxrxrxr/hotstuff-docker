#!/bin/bash
# emergency_debug.sh

echo "🔥 紧急诊断 Ordering1 阶段"


echo "🔍 调试时间戳收集问题"

echo "5. 检查时间戳累积:"
docker-compose logs | grep "新增时间戳.*总计" | tail -10

echo "6. 检查收集进度（应该看到递增）:"
docker-compose logs | grep "时间戳收集进度" | tail -15

echo "7. 检查是否达到要求（应该看到3/3）:"
docker-compose logs | grep "时间戳收集进度.*3/3" | tail -5

echo "8. 检查Ordering2阶段:"
docker-compose logs | grep "收集完成.*开始Ordering2" | tail -5

echo "9. 检查中位数计算:"
docker-compose logs | grep "中位数时间戳" | tail -5

echo "10. 实时监控修复效果:"
docker-compose logs -f | grep -E "(新增时间戳.*总计|时间戳收集进度|收集完成.*开始Ordering2|中位数时间戳)"
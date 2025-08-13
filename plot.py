import re
import matplotlib.pyplot as plt
import numpy as np

def parse_log_file(filename):
    """
    解析log文件，提取Height和TxCount数据
    """
    heights = []
    tx_counts = []
    
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                if line:  # 跳过空行
                    # 使用正则表达式提取Height和TxCount的值
                    height_match = re.search(r'Height:\s*(\d+)', line)
                    txcount_match = re.search(r'TxCount:\s*(\d+)', line)
                    
                    if height_match and txcount_match:
                        height = int(height_match.group(1))
                        tx_count = int(txcount_match.group(1))
                        
                        heights.append(height)
                        tx_counts.append(tx_count)
    
    except FileNotFoundError:
        print(f"错误：找不到文件 '{filename}'")
        return None, None
    except Exception as e:
        print(f"读取文件时出错：{e}")
        return None, None
    
    return heights, tx_counts

def plot_txcount_vs_height(heights, tx_counts, filename='log.log'):
    """
    创建Height vs TxCount的变化曲线图
    """
    if not heights or not tx_counts:
        print("没有数据可以绘制")
        return
    
    # 设置中文字体支持
    plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False
    
    # 创建图表
    plt.figure(figsize=(12, 8))
    plt.plot(heights, tx_counts, 'b-', linewidth=2, marker='o', markersize=4, alpha=0.7)
    
    # 设置标题和轴标签
    plt.title(f'TxCount随Height变化曲线 - {filename}', fontsize=16, fontweight='bold')
    plt.xlabel('Height', fontsize=14)
    plt.ylabel('TxCount', fontsize=14)
    
    # 设置坐标轴范围和刻度
    height_min, height_max = min(heights), max(heights)
    txcount_min, txcount_max = min(tx_counts), max(tx_counts)
    
    # 设置X轴范围和刻度
    plt.xlim(height_min, height_max)
    height_range = height_max - height_min
    if height_range > 100:
        height_step = max(1, height_range // 20)  # 约20个刻度
    else:
        height_step = max(1, height_range // 10)  # 约10个刻度
    
    height_ticks = np.arange(height_min, height_max + 1, height_step)
    plt.xticks(height_ticks, rotation=45 if len(height_ticks) > 10 else 0)
    
    # 设置Y轴范围和刻度
    plt.ylim(txcount_min, txcount_max)
    txcount_range = txcount_max - txcount_min
    if txcount_range > 0:
        if txcount_range > 100:
            txcount_step = max(1, txcount_range // 10)  # 约10个刻度
        else:
            txcount_step = max(1, txcount_range // 5)   # 约5个刻度
    else:
        txcount_step = 1
    
    txcount_ticks = np.arange(txcount_min, txcount_max + 1, txcount_step)
    plt.yticks(txcount_ticks)
    
    # 添加网格
    plt.grid(True, alpha=0.3)
    
    # 添加数据信息
    plt.text(0.02, 0.98, f'数据点数: {len(heights)}\nHeight范围: {height_min} - {height_max}\nTxCount范围: {txcount_min} - {txcount_max}', 
             transform=plt.gca().transAxes, fontsize=10, verticalalignment='top',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    # 调整布局
    plt.tight_layout()
    
    # 显示图表
    plt.show()
    
    # # 保存图表
    # output_filename = filename.replace('.log', '_plot.png')
    # plt.savefig(output_filename, dpi=300, bbox_inches='tight')
    # print(f"图表已保存为: {output_filename}")

def main():
    # 指定log文件名
    filename = 'log.log'
    
    print(f"正在处理文件: {filename}")
    
    # 解析log文件
    heights, tx_counts = parse_log_file(filename)
    
    if heights and tx_counts:
        print(f"成功读取 {len(heights)} 条数据")
        print(f"Height范围: {min(heights)} - {max(heights)}")
        print(f"TxCount范围: {min(tx_counts)} - {max(tx_counts)}")
        
        # 创建图表
        plot_txcount_vs_height(heights, tx_counts, filename)
    else:
        print("无法读取数据，请检查文件格式")

if __name__ == "__main__":
    main()
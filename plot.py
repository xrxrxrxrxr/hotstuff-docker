import re
import matplotlib.pyplot as plt
import numpy as np

def parse_log_file(filename):
    """
    Parse the log file and extract Height and TxCount data.
    """
    heights = []
    tx_counts = []
    
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                if line:  # Skip empty lines
                    # Extract Height and TxCount values with a regular expression
                    height_match = re.search(r'Height:\s*(\d+)', line)
                    txcount_match = re.search(r'TxCount:\s*(\d+)', line)
                    
                    if height_match and txcount_match:
                        height = int(height_match.group(1))
                        tx_count = int(txcount_match.group(1))
                        
                        heights.append(height)
                        tx_counts.append(tx_count)
    
    except FileNotFoundError:
        print(f"Error: file '{filename}' not found")
        return None, None
    except Exception as e:
        print(f"Failed to read file: {e}")
        return None, None
    
    return heights, tx_counts

def plot_txcount_vs_height(heights, tx_counts, filename='log.log'):
    """
    Plot the Height vs TxCount curve.
    """
    if not heights or not tx_counts:
        print("No data available to plot")
        return

    # Configure font settings
    plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False

    # Create the chart
    plt.figure(figsize=(12, 8))
    plt.plot(heights, tx_counts, 'b-', linewidth=2, marker='o', markersize=4, alpha=0.7)

    # Set title and axis labels
    plt.title(f'TxCount vs Height - {filename}', fontsize=16, fontweight='bold')
    plt.xlabel('Height', fontsize=14)
    plt.ylabel('TxCount', fontsize=14)

    # Configure axis ranges and ticks
    height_min, height_max = min(heights), max(heights)
    txcount_min, txcount_max = min(tx_counts), max(tx_counts)

    # Configure X-axis range and ticks
    plt.xlim(height_min, height_max)
    height_range = height_max - height_min
    if height_range > 100:
        height_step = max(1, height_range // 20)  # ~20 ticks
    else:
        height_step = max(1, height_range // 10)  # ~10 ticks
    
    height_ticks = np.arange(height_min, height_max + 1, height_step)
    plt.xticks(height_ticks, rotation=45 if len(height_ticks) > 10 else 0)
    
    # Configure Y-axis range and ticks
    plt.ylim(txcount_min, txcount_max)
    txcount_range = txcount_max - txcount_min
    if txcount_range > 0:
        if txcount_range > 100:
            txcount_step = max(1, txcount_range // 10)  # ~10 ticks
        else:
            txcount_step = max(1, txcount_range // 5)   # ~5 ticks
    else:
        txcount_step = 1
    
    txcount_ticks = np.arange(txcount_min, txcount_max + 1, txcount_step)
    plt.yticks(txcount_ticks)
    
    # Add a grid
    plt.grid(True, alpha=0.3)

    # Add dataset summary
    plt.text(0.02, 0.98, f'Data points: {len(heights)}\nHeight range: {height_min} - {height_max}\nTxCount range: {txcount_min} - {txcount_max}', 
             transform=plt.gca().transAxes, fontsize=10, verticalalignment='top',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))

    # Adjust layout
    plt.tight_layout()

    # Display the chart
    plt.show()

    # # Save the chart
    # output_filename = filename.replace('.log', '_plot.png')
    # plt.savefig(output_filename, dpi=300, bbox_inches='tight')
    # print(f"Chart saved as: {output_filename}")

def main():
    # Specify the log file name
    filename = 'log.log'

    print(f"Processing file: {filename}")

    # Parse the log file
    heights, tx_counts = parse_log_file(filename)

    if heights and tx_counts:
        print(f"Read {len(heights)} records successfully")
        print(f"Height range: {min(heights)} - {max(heights)}")
        print(f"TxCount range: {min(tx_counts)} - {max(tx_counts)}")

        # Generate the chart
        plot_txcount_vs_height(heights, tx_counts, filename)
    else:
        print("Failed to read data; please check the file format")

if __name__ == "__main__":
    main()

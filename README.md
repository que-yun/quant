# 量化交易系统使用说明

## 环境要求

- Python 3.9
- Conda 环境管理工具

## 环境配置步骤

1. 创建并激活 conda 环境
```bash
# 创建名为 quant 的 Python 3.9 环境
conda create -n quant python=3.9

# 激活环境
conda activate quant
```

2. 安装依赖包
```bash
# 在项目根目录下安装所需依赖
pip install -r requirements.txt
```

## 运行说明

### 1. 环境测试
运行环境测试脚本，确保所有依赖正确安装：
```bash
python env_test_demo.py
```

### 2. 启动量化交易系统
运行主程序：
```bash
python main.py
```

### 3. 数据初始化
首次运行系统前，需要执行数据初始化脚本以获取历史数据：
```bash
python -m modules.data.data_scheduler
```

### 4. 回测可视化
运行回测可视化程序：
```bash
python -m modules.strategy.backtest_visualization
```

## 运行结果

- 回测结果将保存为图表文件：
  - `backtest_visualization.png`：详细的回测过程可视化
  - `backtest_summary.png`：回测结果汇总
  - `backtest_result.png`：单个策略的回测结果

- 日志文件：
  - 系统运行日志将保存在 `trading.log` 文件中

## 注意事项

1. 确保系统中已安装 Conda
2. 运行前请确保已激活 quant 环境：`conda activate quant`
3. 首次运行时需要等待数据下载和初始化
4. 如遇到中文显示问题，请确保系统安装了相应的中文字体（如 PingFang SC 或 Heiti TC）
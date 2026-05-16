# LOF032_data_processor.py - 数据处理模块
import os
import re
from datetime import datetime
import pandas as pd
import sqlite3

# 全局共享数据库路径 (动态获取项目根目录下的 database/arb_master.db)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHARED_DB_PATH = os.path.join(BASE_DIR, "database", "arb_master.db")

class DataProcessor:
    """数据处理类"""
    
    def __init__(self, data_dir):
        """初始化数据处理器"""
        self.data_dir = data_dir

    def _infer_year(self, series):
        """从日期列推断年份（优先使用已有完整年份，否则使用当前年份）"""
        try:
            for v in series.dropna().astype(str):
                m = re.match(r'^(\d{4})[-/]', v.strip())
                if m:
                    return int(m.group(1))
        except Exception:
            pass
        return datetime.now().year

    def _normalize_date_column(self, df, col_name='date'):
        """统一日期列格式，兼容YYYY-MM-DD与MM-DD"""
        if col_name not in df.columns:
            return df
        series = df[col_name].astype(str).str.strip()
        inferred_year = self._infer_year(series)
        # 对MM-DD补全年份
        def _fix_date(x):
            if len(x) == 5 and x[2] == '-':
                return f"{inferred_year}-{x}"
            return x
        series = series.apply(_fix_date)
        df[col_name] = pd.to_datetime(series, errors='coerce')
        return df
    
    def read_lof_data(self, fund_code):
        """读取LOF基金数据"""
        # 尝试读取扩展后的LOF历史数据文件（包含静态官方估值）
        filename = f"LOF_{fund_code}_history.csv"
        file_path = os.path.join(self.data_dir, filename)
        table_name = f"fund_history_{fund_code}"
        df = pd.DataFrame()
        
        try:
            conn = sqlite3.connect(SHARED_DB_PATH)
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            conn.close()
        except Exception as e:
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path, encoding='utf-8-sig')
                except Exception as e2:
                    print(f"读取文件 {filename} 失败: {e2}")
                    
        if not df.empty:
            try:
                # 确保日期列存在
                if 'date' not in df.columns:
                    # 尝试其他可能的日期列名
                    for col in ['Date', '日期']:
                        if col in df.columns:
                            df.rename(columns={col: 'date'}, inplace=True)
                            break
                
                if 'date' in df.columns:
                    df = self._normalize_date_column(df, 'date')
                    
                    # 确保必要的列存在
                    if 'nav' not in df.columns:
                        for col in ['NAV', '净值', 'LOF净值']:
                            if col in df.columns:
                                df.rename(columns={col: 'nav'}, inplace=True)
                                break
                    
                    if 'close' not in df.columns:
                        for col in ['Close', '收盘价', 'LOF交易价格', 'LOF交易价']:
                            if col in df.columns:
                                df.rename(columns={col: 'close'}, inplace=True)
                                break
                    
                    if 'static_valuation' not in df.columns:
                        # 兼容新旧版字段命名
                        for col in ['ETF静态估值', '静态官方估值']:
                            if col in df.columns:
                                df.rename(columns={col: 'static_valuation'}, inplace=True)
                                break
                    
                    # 提取纯指数估值数据
                    if 'index_valuation' not in df.columns:
                        for col in ['指数静态估值']:
                            if col in df.columns:
                                df.rename(columns={col: 'index_valuation'}, inplace=True)
                                break
                                
                    # 处理纯指数估值列中的无效值
                    if 'index_valuation' in df.columns:
                        df['index_valuation'] = df['index_valuation'].replace(['n', '无', 'N/A', 'NA'], pd.NA)
                    
                    # 处理静态官方估值列中的无效值
                    if 'static_valuation' in df.columns:
                        # 将'n'和'无'等无效值转换为NaN
                        df['static_valuation'] = df['static_valuation'].replace(['n', '无', 'N/A', 'NA'], pd.NA)
                        # 尝试将列转换为数值类型
                        try:
                            df['static_valuation'] = pd.to_numeric(df['static_valuation'], errors='coerce')
                        except Exception as e:
                            pass
                    
                    if 'exchange_rate' not in df.columns:
                        for col in ['人民币中间价']:
                            if col in df.columns:
                                df.rename(columns={col: 'exchange_rate'}, inplace=True)
                                break
                    
                    # 过滤掉日期为空的行
                    df = df[df['date'].notna()]
                    
                    if len(df) > 0:
                        return df.sort_values('date', ascending=False).reset_index(drop=True)
            except Exception as e:
                print(f"读取文件 {filename} 失败: {e}")
        else:
            print(f"警告: 找不到LOF历史数据文件: {file_path}")
            print(f"读取 SQLite 表 {table_name} 失败: {e}")
        return pd.DataFrame()
    
    def read_basic_data(self):
        """读取基础数据（从 SQLite 并行读取后合并输出，兼容旧版 CSV 结构）"""
        df = pd.DataFrame()
        try:
            conn = sqlite3.connect(SHARED_DB_PATH)
            # 1. 读取汇率
            fx_df = pd.read_sql("SELECT date, usd_cny_mid as 人民币中间价 FROM exchange_rate", conn)
            if not fx_df.empty:
                df = fx_df
            
            # 2. 读取校准常量
            calib_df = pd.read_sql("SELECT date, symbol, calibration FROM futures_daily WHERE calibration IS NOT NULL", conn)
            if not calib_df.empty:
                calib_pivot = calib_df.pivot(index='date', columns='symbol', values='calibration').reset_index()
                calib_pivot.rename(columns={'GC': '黄金校准', 'CL': '原油校准'}, inplace=True)
                if df.empty:
                    df = calib_pivot
                else:
                    df = pd.merge(df, calib_pivot, on='date', how='outer')

            # 3. 读取期货结算价
            fut_df = pd.read_sql("SELECT date, symbol, settle_price FROM futures_daily WHERE settle_price IS NOT NULL", conn)
            if not fut_df.empty:
                fut_pivot = fut_df.pivot(index='date', columns='symbol', values='settle_price').reset_index()
                rename_map = {c: f"{c}_settle" for c in fut_pivot.columns if c != 'date'}
                fut_pivot.rename(columns=rename_map, inplace=True)
                if df.empty:
                    df = fut_pivot
                else:
                    df = pd.merge(df, fut_pivot, on='date', how='outer')

            # 4. 读取 ETF 价格
            etf_df = pd.read_sql("SELECT date, symbol, price FROM usa_etf_daily_prices", conn)
            if not etf_df.empty:
                etf_pivot = etf_df.pivot(index='date', columns='symbol', values='price').reset_index()
                if df.empty:
                    df = etf_pivot
                else:
                    df = pd.merge(df, etf_pivot, on='date', how='outer')

            conn.close()
            
            if not df.empty:
                df = self._normalize_date_column(df, 'date')
                return df.sort_values('date', ascending=False).reset_index(drop=True)
        except Exception as e:
            print(f"读取 SQLite 基础数据表失败: {e}")
            
        return pd.DataFrame()
    
    def get_base_date_info(self, historical_data):
        """获取基准日期信息
        
        Args:
            historical_data: 历史数据
            
        Returns:
            tuple: (base_date, base_nav, base_row) 如果没有找到有效的基准日期，返回(None, None, None)
        """
        if historical_data is None or len(historical_data) == 0:
            return None, None, None
        
        # 找到有净值的最新日期（优先使用标准化列名）
        date_col = 'date' if 'date' in historical_data.columns else ('日期' if '日期' in historical_data.columns else None)
        nav_col = 'nav' if 'nav' in historical_data.columns else ('LOF净值' if 'LOF净值' in historical_data.columns else '净值')
        if date_col is None or nav_col not in historical_data.columns:
            return None, None, None
        for _, row in historical_data.iterrows():
            nav_val = row.get(nav_col, None)
            if nav_val and not pd.isna(nav_val):
                base_date = row.get(date_col)
                base_nav = nav_val
                base_row = row
                return base_date, base_nav, base_row
        
        return None, None, None

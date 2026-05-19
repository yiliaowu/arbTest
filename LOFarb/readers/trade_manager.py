import os
import sys
import time
import socket
import threading

class TradeManager:
    """A股/LOF统一交易接口管理器"""
    def __init__(self, preload_brokers=None):
        self.tdx_available = False
        self.tq = None
        self.tdx_account_id = None
        
        self.xtquant_available = False
        self.xt_trader = None
        self.xt_account = None
        self.xtconstant = None
        self._broker_lock = threading.Lock()
        self._tdx_init_attempted = False
        self._guojin_init_attempted = False

        # 外部交易库按通道懒加载，避免银河 Socket 下单被无关通道初始化拖慢。
        if preload_brokers:
            self.ensure_brokers(preload_brokers)

    def ensure_brokers(self, brokers):
        if isinstance(brokers, str):
            brokers = [brokers]
        broker_set = set(brokers or [])
        with self._broker_lock:
            if 'tdx' in broker_set and not self._tdx_init_attempted:
                self._tdx_init_attempted = True
                self._init_tdx()
            if 'guojin_qmt' in broker_set and not self._guojin_init_attempted:
                self._guojin_init_attempted = True
                self._init_guojin_qmt()

    def _init_tdx(self):
        try:
            tdx_api_path = r'D:\new_tdx64\PYPlugins\user'
            if os.path.exists(tdx_api_path) and tdx_api_path not in sys.path:
                sys.path.append(tdx_api_path)
            from tqcenter import tq
            self.tq = tq
            
            # 通达信 API 需要先初始化连接（传入任意有效路径）
            tdx_bin_path = r'D:\new_tdx64\T0002\bin'
            if os.path.exists(tdx_bin_path):
                self.tq.initialize(tdx_bin_path)
            else:
                # 如果主程序路径不存在，使用插件路径作为备选
                self.tq.initialize(tdx_api_path)
            
            # 获取交易账户句柄
            self.tdx_account_id = self.tq.stock_account(account='', account_type='stock')
            if self.tdx_account_id is None or self.tdx_account_id < 0:
                raise RuntimeError(f"获取通达信交易账户句柄失败: {self.tdx_account_id}")
            self.tdx_available = True
            print(f"SUCCESS: [TradeManager] 已挂载【通达信】交易与极速行情模块 (账户ID:{self.tdx_account_id})")
        except Exception as e:
            self.tdx_available = False
            self.tq = None
            self.tdx_account_id = None
            print(f"INFO: [TradeManager] 未检测到通达信环境或账户句柄获取失败，已跳过: {e}")

    def _init_guojin_qmt(self):
        try:
            # ====================== 国金 QMT 路径与环境配置 ======================
            QMT_INSTALL_PATH = r"D:\国金证券QMT交易端"
            if os.path.exists(QMT_INSTALL_PATH):
                if QMT_INSTALL_PATH not in sys.path:
                    sys.path.append(QMT_INSTALL_PATH)
                    sys.path.append(os.path.join(QMT_INSTALL_PATH, "lib"))
                    sys.path.append(os.path.join(QMT_INSTALL_PATH, "bin.x64"))
                    sys.path.append(os.path.join(QMT_INSTALL_PATH, "bin.x64", "Lib", "site-packages"))
                
                from xtquant import xttrader, xtconstant
                from xtquant.xttype import StockAccount
                
                qmt_path = os.path.join(QMT_INSTALL_PATH, 'userdata_mini')
                session_id = int(time.time())
                self.xt_trader = xttrader.XtQuantTrader(qmt_path, session_id)
                self.xt_account = StockAccount('8890282471')
                self.xtconstant = xtconstant
                
                self.xt_trader.start()
                connect_result = self.xt_trader.connect()
                if connect_result == 0:
                    self.xt_trader.subscribe(self.xt_account)
                    self.xtquant_available = True
                    print(f"SUCCESS: [TradeManager] 已挂载【国金MiniQMT】原生直连通道 (账号:{self.xt_account.account_id})")
                else:
                    print(f"WARNING: [TradeManager] 国金QMT客户端连接失败 (错误码: {connect_result})")
        except Exception as e:
            print(f"INFO: [TradeManager] 国金QMT模块跳过加载: {e}")

    def send_order(self, broker, action, symbol, volume, price):
        """暴露给外部的统一路由函数"""
        if broker == 'yinhe_qmt':
            try:
                start_ts = time.perf_counter()
                cmd_str = f"{action},{symbol},{volume},{price}\n"
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.settimeout(2.0)
                connect_start = time.perf_counter()
                client.connect(('127.0.0.1', 8888))
                connect_ms = (time.perf_counter() - connect_start) * 1000
                send_start = time.perf_counter()
                client.sendall(cmd_str.encode('utf-8'))
                send_ms = (time.perf_counter() - send_start) * 1000
                recv_start = time.perf_counter()
                response = client.recv(1024).decode('utf-8')
                recv_ms = (time.perf_counter() - recv_start) * 1000
                client.close()
                total_ms = (time.perf_counter() - start_ts) * 1000
                timing = f"耗时: 总{total_ms:.0f}ms/连接{connect_ms:.0f}ms/发送{send_ms:.0f}ms/等待QMT{recv_ms:.0f}ms"
                return True, f"银河QMT(Socket)返回: {response.strip()} ({timing})"
            except ConnectionRefusedError:
                return False, "银河QMT未开启或 8888 桥接策略未运行"
            except Exception as e:
                return False, f"银河QMT下单异常: {str(e)}"
                
        elif broker == 'guojin_qmt':
            self.ensure_brokers('guojin_qmt')
            if not self.xtquant_available: return False, "国金 QMT 底层环境未就绪"
            try:
                order_type = self.xtconstant.STOCK_BUY if action == 'BUY' else self.xtconstant.STOCK_SELL
                seq = self.xt_trader.order_stock(self.xt_account, symbol, order_type, volume, self.xtconstant.FIX_PRICE, price, 'LOF_Arb', 'Strategy')
                return True, f"国金QMT(原生)委托成功, 编号: {seq}"
            except Exception as e:
                return False, f"国金QMT下单异常: {str(e)}"
                
        elif broker == 'tdx':
            self.ensure_brokers('tdx')
            if not self.tdx_available: return False, "通达信接口未就绪"
            if self.tdx_account_id is None or self.tdx_account_id < 0:
                return False, "通达信交易账户未就绪"
            try:
                # 通达信 order_stock 需要完整的市场后缀代码，如 165513.SZ 或 600519.SH
                full_symbol = symbol.strip().upper()
                if '.' not in full_symbol:
                    return False, "通达信下单需传入完整代码后缀，如 165513.SZ"
                order_type = 0 if action == 'BUY' else 1
                res = self.tq.order_stock(self.tdx_account_id, full_symbol, order_type, volume, 0, price, 0)
                return True, f"通达信返回: {res}"
            except Exception as e:
                return False, f"通达信下单异常: {str(e)}"
                
        return False, f"未知的通道标识: {broker}"

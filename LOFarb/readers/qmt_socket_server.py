# encoding: gbk
# =================================================================
# v4.0 绝杀版 - 银河QMT Socket Server端策略
# 【重要】此文件是运行在银河QMT客户端内部的策略代码
# 不是Python主程序调用的，请在QMT策略编辑器中加载此代码
# =================================================================
# 功能：
# - 监听 127.0.0.1:8888 端口
# - 支持 PING 心跳检测
# - 支持 SUBSCRIBE 订阅股票代码
# - 实时推送 TICK 行情数据
# - 支持 BUY / SELL 下单指令
# - 使用互斥锁保护 QMT 底层 API，防止死锁
# - 下单时临时暂停行情推送抢锁，保证交易指令优先响应
# g_account_id = '332600089412'
# =================================================================

import socket
import threading
import time

g_context = None
g_api_lock = threading.Lock()
g_account_id = ""
g_active_clients = []
g_clients_lock = threading.Lock()
g_subscribed_stocks = set()
g_order_pending = threading.Event()
g_last_push_ts = 0
g_push_interval = 2.0
g_order_cooldown_until = 0
SERVER_VERSION = "arbTest-qmt-callback-order-20260519"
g_auto_push_ticks = False
g_order_queue = []
g_order_queue_lock = threading.Lock()
g_order_seq = 0

def client_handler(conn, addr):
    buffer = ""
    try:
        while True:
            data = conn.recv(1024).decode('utf-8')
            if not data:
                break
            buffer += data
            while '\n' in buffer:
                cmd_str, buffer = buffer.split('\n', 1)
                if cmd_str:
                    process_command_sync(conn, cmd_str.strip())
    except Exception:
        pass
    finally:
        with g_clients_lock:
            if conn in g_active_clients:
                g_active_clients.remove(conn)
        conn.close()

def process_command_sync(conn, cmd_str):
    global g_context, g_account_id, g_subscribed_stocks, g_auto_push_ticks
    parts = cmd_str.split(',')
    action = parts[0].upper()

    if action == 'PING':
        try:
            conn.sendall(b'PONG\n')
        except Exception:
            pass

    elif action == 'VERSION':
        try:
            conn.sendall((SERVER_VERSION + '\n').encode('utf-8'))
        except Exception:
            pass

    elif action == 'TICK_PUSH_ON':
        g_auto_push_ticks = True
        try:
            conn.sendall(b'TICK_PUSH_ON_OK\n')
        except Exception:
            pass

    elif action == 'TICK_PUSH_OFF':
        g_auto_push_ticks = False
        try:
            conn.sendall(b'TICK_PUSH_OFF_OK\n')
        except Exception:
            pass

    elif action == 'QUERY_TICK' and len(parts) >= 2:
        code = parts[1].strip()
        response = f"TICK_RESULT,{code} | 暂无数据"
        if g_context:
            with g_api_lock:
                try:
                    ticks = g_context.get_full_tick([code])
                    if code in ticks:
                        tick = ticks[code]
                        response = f"TICK_RESULT,{code} | 最新/收盘价:{tick.get('lastPrice', 0)} | 昨收:{tick.get('lastClose', 0)}"
                except Exception as e:
                    response = f"TICK_RESULT,{code} | 查询异常: {e}"
        try:
            conn.sendall((response + '\n').encode('utf-8'))
        except Exception:
            pass

    elif action in ['BUY', 'SELL'] and len(parts) >= 4:
        global g_order_cooldown_until, g_order_seq
        start_ts = time.time()
        code, volume, price = parts[1], int(parts[2]), float(parts[3])
        opType = 23 if action == 'BUY' else 24
        # passorder 返回很快，但 QMT 内部真实送单是异步队列；先停行情抢锁，
        # 给交易线程一个干净窗口，避免订单在 QMT 内部排队几十秒。
        g_order_cooldown_until = time.time() + 10.0
        event = threading.Event()
        order_item = {
            'opType': opType,
            'action': action,
            'code': code,
            'volume': volume,
            'price': price,
            'event': event,
            'ok': False,
            'error': '',
            'elapsed_ms': 0,
        }
        with g_order_queue_lock:
            g_order_seq += 1
            order_item['seq'] = g_order_seq
            g_order_queue.append(order_item)

        event.wait(3.0)
        try:
            elapsed_ms = int((time.time() - start_ts) * 1000)
            if order_item.get('ok'):
                conn.sendall(f"OK,{order_item.get('elapsed_ms', elapsed_ms)}ms,queued,{elapsed_ms}ms\n".encode('utf-8'))
            elif order_item.get('error'):
                conn.sendall(f"ERROR,{order_item.get('error')},queued,{elapsed_ms}ms\n".encode('utf-8'))
            else:
                conn.sendall(f"ERROR,QMT_ORDER_CALLBACK_TIMEOUT,queued,{elapsed_ms}ms\n".encode('utf-8'))
        except Exception:
            pass

    elif action == 'SUBSCRIBE' and len(parts) > 1:
        new_stocks = [p.strip() for p in parts[1:] if p.strip()]
        g_subscribed_stocks.update(new_stocks)
        with g_clients_lock:
            if conn not in g_active_clients:
                g_active_clients.append(conn)

        try:
            conn.sendall(b'SUBSCRIBE_OK\n')
        except Exception:
            pass
        try:
            conn.sendall(f"DEBUG, 开始为您提取 {new_stocks} 的盘口数据...\n".encode('utf-8'))
        except Exception:
            pass
        if g_auto_push_ticks:
            push_ticks()  # 核心：订阅后立即推送一次，破除周末休眠

def socket_server_thread():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server.bind(('127.0.0.1', 8888))
        server.listen(5)
        print("[OK] 银河QMT Socket Server Started. Listening on 8888...")
    except Exception as e:
        print(f"[ERROR] 致命错误：端口 8888 被占用！请彻底重启QMT软件！详细报错: {e}")
        return

    while True:
        try:
            conn, addr = server.accept()
            t = threading.Thread(target=client_handler, args=(conn, addr))
            t.setDaemon(True)
            t.start()
        except Exception:
            time.sleep(1)

def broadcast_message(msg):
    with g_clients_lock:
        dead_clients = []
        for client_conn in g_active_clients:
            try:
                client_conn.sendall(msg.encode('utf-8'))
            except Exception:
                dead_clients.append(client_conn)
        for dead in dead_clients:
            g_active_clients.remove(dead)

def init(ContextInfo):
    global g_account_id, g_context
    print("\n[策略日志] 加载 v4.0 绝杀版 Socket 策略（下单优先版）...")
    g_account_id = '332600089412'
    g_context = ContextInfo
    ContextInfo.set_account(g_account_id)

    t = threading.Thread(target=socket_server_thread)
    t.setDaemon(True)
    t.start()

    ContextInfo.run_time("check_tasks", "1nSecond", "2020-01-01 09:30:00")

def push_ticks():
    global g_context, g_subscribed_stocks, g_last_push_ts, g_order_cooldown_until
    if not g_context or not g_subscribed_stocks or len(g_active_clients) == 0:
        return
    if g_order_pending.is_set():
        return
    now = time.time()
    if now < g_order_cooldown_until:
        return
    if now - g_last_push_ts < g_push_interval:
        return
    g_last_push_ts = now
    try:
        # QMT API 调用必须串行保护；拿到 ticks 后立刻释放锁，广播不要占用交易锁。
        with g_api_lock:
            if g_order_pending.is_set():
                return
            ticks = g_context.get_full_tick(list(g_subscribed_stocks))

        for code, tick in ticks.items():
            if not tick or not isinstance(tick, dict):
                continue

            # 终极防御：处理 QMT 返回 Tuple 或 None 导致的拼接崩溃问题。
            def safe_list(val):
                if isinstance(val, (list, tuple)):
                    return list(val) + [0] * 5
                return [0] * 5

            ap = safe_list(tick.get('askPrice'))
            av = safe_list(tick.get('askVol'))
            bp = safe_list(tick.get('bidPrice'))
            bv = safe_list(tick.get('bidVol'))

            msg = f"TICK,{code},{tick.get('lastPrice', 0)},{tick.get('volume', 0)},{ap[0]},{av[0]},{ap[1]},{av[1]},{bp[0]},{bv[0]},{bp[1]},{bv[1]},{tick.get('timetag', '')}\n"
            broadcast_message(msg)
    except Exception as e:
        broadcast_message(f"ERROR, push_ticks 发生错误: {e}\n")
        print(f"[推流异常] push_ticks 发生错误: {e}")

def process_pending_orders():
    global g_order_cooldown_until
    while True:
        with g_order_queue_lock:
            if not g_order_queue:
                return
            order_item = g_order_queue.pop(0)
        start_ts = time.time()
        g_order_pending.set()
        g_order_cooldown_until = time.time() + 10.0
        try:
            with g_api_lock:
                msg = f"Socket_{order_item['action']}_{order_item['code']}_{order_item.get('seq', 0)}"
                passorder(order_item['opType'], 1101, g_account_id, order_item['code'], 11, order_item['price'], order_item['volume'], 'SocketTrade', 1, msg, g_context)
            order_item['ok'] = True
        except Exception as e:
            order_item['error'] = str(e)
            print(f"Passorder Error: {e}")
        finally:
            order_item['elapsed_ms'] = int((time.time() - start_ts) * 1000)
            g_order_pending.clear()
            g_order_cooldown_until = time.time() + 10.0
            order_item['event'].set()

def check_tasks(ContextInfo):
    process_pending_orders()
    if g_auto_push_ticks:
        push_ticks()

def handlebar(ContextInfo):
    process_pending_orders()
    if g_auto_push_ticks:
        push_ticks()

def orderError_callback(ContextInfo, passOrderInfo, msg):
    pass

def deal_callback(ContextInfo, dealInfo):
    pass

def order_callback(ContextInfo, orderInfo):
    pass


import sys
import os
tdx_install_path = r"D:\new_tdx64"
pyplugins_user_path = os.path.join(tdx_install_path, "PYPlugins", "user")
sys.path.insert(0, pyplugins_user_path)

# 使用tqcenter的API函数查看平安银行日线数据示例
from tqcenter import tq
from tqcenter import tqconst
#初始化
tq.initialize(__file__) #所有策略连接通达信客户端都必须调用此函数进行初始化

# 获取账户句柄
myAccount = tq.stock_account(account="", account_type="STOCK")
print("account_id:", myAccount)

# 参考 tdxdata_test.py 的下单接口：卖出 1 手 162411.SZ
# 1 手 = 100 股
order_res = tq.order_stock(
        account_id=myAccount,
        stock_code="162411.SZ",
        order_type=tqconst.STOCK_SELL,
        order_volume=100,
        price_type=tqconst.PRICE_MY,
        price=0
    )
print(order_res)


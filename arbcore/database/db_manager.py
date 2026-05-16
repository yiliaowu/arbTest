# -*- coding: utf-8 -*-
import os
import threading
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

import pandas as pd
import pymysql
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseManager:
    """MySQL-only database manager for the LOF arbitrage system."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
    ):
        self.host = host or os.getenv("ARB_MYSQL_HOST", "localhost")
        self.port = int(port or os.getenv("ARB_MYSQL_PORT", "3306"))
        self.user = user or os.getenv("ARB_MYSQL_USER", "root")
        self.password = password if password is not None else os.getenv("ARB_MYSQL_PASSWORD", "P@ssw0rd")
        self.database = database or os.getenv("ARB_MYSQL_DATABASE", "arb_master")
        self.charset = os.getenv("ARB_MYSQL_CHARSET", "utf8mb4")
        self.lock = threading.Lock()
        self._engine: Engine | None = None

        self._ensure_database()
        self.init_db()

    def _server_conn(self):
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            charset=self.charset,
            autocommit=False,
        )

    def _ensure_database(self):
        conn = self._server_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{self.database}` "
                    "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
            conn.commit()
        finally:
            conn.close()

    def _get_conn(self):
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset=self.charset,
            autocommit=False,
        )

    def get_engine(self) -> Engine:
        if self._engine is None:
            url = URL.create(
                "mysql+pymysql",
                username=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database,
                query={"charset": self.charset},
            )
            self._engine = create_engine(url, pool_pre_ping=True, future=True)
        return self._engine

    def init_db(self):
        ddl = [
            """
            CREATE TABLE IF NOT EXISTS fund_data (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                date VARCHAR(10) NOT NULL,
                fund_code VARCHAR(32) NOT NULL,
                price DOUBLE NULL,
                nav DOUBLE NULL,
                premium DOUBLE NULL,
                static_val DOUBLE NULL,
                val_error DOUBLE NULL,
                created_at DATETIME NULL,
                UNIQUE KEY uq_fund_data_date_code (date, fund_code)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS system_health (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                component VARCHAR(128) NOT NULL,
                status VARCHAR(64),
                message TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_health_component (component)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS exchange_rate (
                date VARCHAR(10) PRIMARY KEY,
                usd_cny_mid DOUBLE,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS usa_etf_daily_prices (
                date VARCHAR(10) NOT NULL,
                symbol VARCHAR(64) NOT NULL,
                price DOUBLE,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (date, symbol),
                INDEX idx_etf_prices_date (date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS index_daily (
                date VARCHAR(10) NOT NULL,
                symbol VARCHAR(64) NOT NULL,
                price DOUBLE,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (date, symbol)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS futures_daily (
                date VARCHAR(10) NOT NULL,
                symbol VARCHAR(64) NOT NULL,
                settle_price DOUBLE,
                calibration DOUBLE,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (date, symbol)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS fund_basket_weights (
                date VARCHAR(10) NOT NULL,
                fund_code VARCHAR(32) NOT NULL,
                underlying_symbol VARCHAR(64) NOT NULL,
                weight DOUBLE,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (date, fund_code, underlying_symbol),
                INDEX idx_fund_basket (fund_code, date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS fund_daily_factors (
                date VARCHAR(10) NOT NULL,
                fund_code VARCHAR(32) NOT NULL,
                calibration DOUBLE,
                hedge DOUBLE,
                position DOUBLE,
                nav DOUBLE,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (date, fund_code),
                INDEX idx_fund_code_date (fund_code, date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS raw_api_data (
                date VARCHAR(10) NOT NULL,
                source VARCHAR(128) NOT NULL,
                raw_content LONGTEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (date, source)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS access_sync_status (
                sync_date VARCHAR(10) NOT NULL,
                access_source VARCHAR(128) NOT NULL,
                sync_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (sync_date, access_source)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS etf_raw_api_data (
                date VARCHAR(10) NOT NULL,
                source VARCHAR(128) NOT NULL,
                raw_content LONGTEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (date, source)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS etf_rotation_list (
                group_id INT,
                lof_code VARCHAR(32) NOT NULL,
                lof_name VARCHAR(255),
                etf_code VARCHAR(32) NOT NULL,
                etf_name VARCHAR(255),
                track_index VARCHAR(255),
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (lof_code, etf_code)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS jsl_fund_list (
                category VARCHAR(128),
                fund_code VARCHAR(32) PRIMARY KEY,
                fund_name VARCHAR(255),
                related_index VARCHAR(255)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS fund_purchase_status (
                fund_code VARCHAR(32) PRIMARY KEY,
                purchase_status VARCHAR(128),
                redemption_status VARCHAR(128),
                purchase_fee VARCHAR(64),
                redemption_fee VARCHAR(64),
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
        ]
        with self.lock:
            conn = self._get_conn()
            try:
                with conn.cursor() as cursor:
                    for sql in ddl:
                        cursor.execute(sql)
                    for old_table in ("futures_data", "future_calibration", "macro_data", "api_sync_status"):
                        cursor.execute(f"DROP TABLE IF EXISTS `{old_table}`")
                conn.commit()
            finally:
                conn.close()

    @staticmethod
    def _execute(conn, sql: str, params=None):
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor

    def _upsert(self, table: str, data: Dict[str, Any], update_cols: List[str] | None = None):
        cols = list(data.keys())
        update_cols = update_cols if update_cols is not None else cols
        col_sql = ", ".join(f"`{c}`" for c in cols)
        placeholders = ", ".join(["%s"] * len(cols))
        update_sql = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in update_cols)
        query = f"INSERT INTO `{table}` ({col_sql}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_sql}"
        conn = self._get_conn()
        try:
            self._execute(conn, query, tuple(data[c] for c in cols))
            conn.commit()
        finally:
            conn.close()

    def save_fund_data(self, date, fund_code, price, nav, premium):
        self._upsert(
            "fund_data",
            {
                "date": date,
                "fund_code": fund_code,
                "price": price,
                "nav": nav,
                "premium": premium,
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            },
        )

    def update_fund_valuation(self, date: str, fund_code: str, static_val: float, val_error: float):
        with self.lock:
            conn = self._get_conn()
            try:
                self._execute(
                    conn,
                    "UPDATE fund_data SET static_val = %s, val_error = %s WHERE date = %s AND fund_code = %s",
                    (static_val, val_error, date, fund_code),
                )
                conn.commit()
            finally:
                conn.close()

    def upsert_exchange_rate(self, date: str, usd_cny_mid: float):
        self._upsert("exchange_rate", {"date": date, "usd_cny_mid": usd_cny_mid})

    def upsert_futures_daily(self, date: str, symbol: str, settle_price: float = None, calibration: float = None):
        data = {"date": date, "symbol": symbol, "settle_price": settle_price, "calibration": calibration}
        update_cols = [c for c in ("settle_price", "calibration") if data[c] is not None]
        if not update_cols:
            update_cols = ["symbol"]
        self._upsert("futures_daily", data, update_cols=update_cols)

    def upsert_usa_etf_price(self, date: str, symbol: str, price: float):
        self._upsert("usa_etf_daily_prices", {"date": date, "symbol": symbol, "price": price})

    def upsert_etf_price(self, date: str, symbol: str, price: float):
        self.upsert_usa_etf_price(date, symbol, price)

    def upsert_index_price(self, date: str, symbol: str, price: float):
        self._upsert("index_daily", {"date": date, "symbol": symbol, "price": price})

    def upsert_fund_factor(self, date: str, fund_code: str, calibration: float, hedge: float, position: float, nav: float = None):
        self._upsert(
            "fund_daily_factors",
            {
                "date": date,
                "fund_code": fund_code,
                "calibration": calibration,
                "hedge": hedge,
                "position": position,
                "nav": nav,
            },
        )

    def upsert_fund_basket_weight(self, date: str, fund_code: str, underlying_symbol: str, weight: float):
        self._upsert(
            "fund_basket_weights",
            {"date": date, "fund_code": fund_code, "underlying_symbol": underlying_symbol, "weight": weight},
        )

    def get_latest_fund_factor(self, fund_code: str):
        conn = self._get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT date, calibration, hedge, position
                    FROM fund_daily_factors
                    WHERE fund_code = %s
                    ORDER BY date DESC LIMIT 1
                    """,
                    (fund_code,),
                )
                result = cursor.fetchone()
        finally:
            conn.close()
        if result:
            return {"date": result[0], "calibration": result[1], "hedge": result[2], "position": result[3]}
        return None

    def get_fund_basket(self, date: str, fund_code: str):
        conn = self._get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT underlying_symbol, weight FROM fund_basket_weights WHERE date = %s AND fund_code = %s",
                    (date, fund_code),
                )
                results = cursor.fetchall()
        finally:
            conn.close()
        return [{"symbol": row[0], "weight": row[1]} for row in results]

    def save_raw_api_data(self, date: str, source: str, raw_content: str):
        self._upsert("raw_api_data", {"date": date, "source": source, "raw_content": raw_content})

    def get_raw_api_data(self, date: str, source: str):
        conn = self._get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT raw_content FROM raw_api_data WHERE date = %s AND source = %s", (date, source))
                result = cursor.fetchone()
        finally:
            conn.close()
        return result[0] if result else None

    def save_etf_raw_api_data(self, date: str, source: str, raw_content: str):
        self._upsert("etf_raw_api_data", {"date": date, "source": source, "raw_content": raw_content})

    def get_etf_raw_api_data(self, date: str, source: str):
        conn = self._get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT raw_content FROM etf_raw_api_data WHERE date = %s AND source = %s", (date, source))
                result = cursor.fetchone()
        finally:
            conn.close()
        return result[0] if result else None

    def mark_access_synced(self, sync_date: str, source: str):
        self._upsert("access_sync_status", {"sync_date": sync_date, "access_source": source, "sync_time": datetime.now()})

    def is_access_synced_today(self, sync_date: str, source: str) -> bool:
        conn = self._get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT 1 FROM access_sync_status WHERE sync_date = %s AND access_source = %s",
                    (sync_date, source),
                )
                result = cursor.fetchone()
        finally:
            conn.close()
        return result is not None

    def remove_access_sync_status(self, sync_date: str, source: str):
        with self.lock:
            conn = self._get_conn()
            try:
                self._execute(
                    conn,
                    "DELETE FROM access_sync_status WHERE sync_date = %s AND access_source = %s",
                    (sync_date, source),
                )
                conn.commit()
            finally:
                conn.close()

    def mark_api_synced(self, sync_date: str, source: str):
        self.mark_access_synced(sync_date, source)

    def is_api_synced_today(self, sync_date: str, source: str) -> bool:
        return self.is_access_synced_today(sync_date, source)

    def sync_etf_rotation_list(self, df):
        with self.lock:
            conn = self._get_conn()
            try:
                with conn.cursor() as cursor:
                    cursor.execute("DROP TABLE IF EXISTS etf_rotation_list")
                    cursor.execute(
                        """
                        CREATE TABLE etf_rotation_list (
                            group_id INT,
                            lof_code VARCHAR(32) NOT NULL,
                            lof_name VARCHAR(255),
                            etf_code VARCHAR(32) NOT NULL,
                            etf_name VARCHAR(255),
                            track_index VARCHAR(255),
                            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                            PRIMARY KEY (lof_code, etf_code)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                        """
                    )
                    for _, row in df.iterrows():
                        cursor.execute(
                            """
                            INSERT INTO etf_rotation_list
                            (group_id, lof_code, lof_name, etf_code, etf_name, track_index)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                            (
                                int(row["组别"]),
                                str(row["LOF基金代码"]).split(".")[0].zfill(6),
                                str(row["LOF基金名称"]),
                                str(row["ETF基金代码"]).split(".")[0].zfill(6),
                                str(row["ETF基金名称"]),
                                str(row["跟踪指数"]),
                            ),
                        )
                conn.commit()
                logger.info(f"已将 {len(df)} 条轮动配置同步至 MySQL etf_rotation_list 表。")
            except Exception as e:
                logger.error(f"同步轮动配置池失败: {e}")
            finally:
                conn.close()

    def sync_jsl_fund_list(self, fund_list: List[Dict[str, str]]):
        for item in fund_list:
            self._upsert(
                "jsl_fund_list",
                {
                    "category": item["category"],
                    "fund_code": item["code"],
                    "fund_name": item["name"],
                    "related_index": item.get("related_index", "-"),
                },
            )
        logger.info(f"已将 {len(fund_list)} 条 JSL 配置同步至 MySQL。")

    def get_jsl_fund_list(self) -> List[Dict[str, str]]:
        conn = self._get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT category, fund_code, fund_name, related_index FROM jsl_fund_list")
                results = cursor.fetchall()
        finally:
            conn.close()
        return [{"category": r[0], "code": r[1], "name": r[2], "related_index": r[3]} for r in results]

    def batch_save_fund_purchase_status(self, df):
        records = df.to_records(index=False)
        with self.lock:
            conn = self._get_conn()
            try:
                with conn.cursor() as cursor:
                    cursor.executemany(
                        """
                        INSERT INTO fund_purchase_status
                        (fund_code, purchase_status, redemption_status, purchase_fee, redemption_fee)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            purchase_status = VALUES(purchase_status),
                            redemption_status = VALUES(redemption_status),
                            purchase_fee = VALUES(purchase_fee),
                            redemption_fee = VALUES(redemption_fee)
                        """,
                        records,
                    )
                conn.commit()
                logger.info(f"成功将 {len(df)} 条全市场申赎状态缓存入库。")
            except Exception as e:
                logger.error(f"批量保存 AKShare 申赎状态失败: {e}")
            finally:
                conn.close()

    def get_fund_purchase_status(self, fund_code: str) -> Dict[str, str]:
        conn = self._get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT purchase_status, redemption_status, purchase_fee, redemption_fee
                    FROM fund_purchase_status WHERE fund_code = %s
                    """,
                    (fund_code,),
                )
                r = cursor.fetchone()
        finally:
            conn.close()
        if r:
            return {"purchase_status": r[0], "redemption_status": r[1], "purchase_fee": r[2], "redemption_fee": r[3]}
        return {"purchase_status": "未知", "redemption_status": "未知", "purchase_fee": "0%", "redemption_fee": "0.50%"}

    def get_latest_futures_price(self, symbol: str) -> Optional[float]:
        try:
            conn = self._get_conn()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT settle_price FROM futures_daily WHERE symbol = %s ORDER BY date DESC LIMIT 1",
                        (symbol,),
                    )
                    result = cursor.fetchone()
            finally:
                conn.close()
            return result[0] if result and result[0] is not None else None
        except Exception as e:
            logger.error(f"获取期货价格失败: {e}")
            return None

    def get_latest_fund_price(self, code: str) -> Optional[Dict[str, Any]]:
        try:
            conn = self._get_conn()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT fund_code, price, nav, premium, created_at, date
                        FROM fund_data
                        WHERE fund_code = %s
                        ORDER BY date DESC LIMIT 1
                        """,
                        (code,),
                    )
                    result = cursor.fetchone()
            finally:
                conn.close()
            if result:
                return {
                    "code": result[0],
                    "price": result[1],
                    "nav": result[2],
                    "premium": result[3],
                    "timestamp": result[4],
                    "date": result[5],
                }
            return None
        except Exception as e:
            logger.error(f"获取 LOF 价格失败: {e}")
            return None

    def get_health_status(self, component: str = None) -> List[Dict[str, Any]]:
        try:
            conn = self._get_conn()
            try:
                with conn.cursor() as cursor:
                    if component:
                        cursor.execute(
                            """
                            SELECT component, status, message, timestamp
                            FROM system_health
                            WHERE component = %s
                            ORDER BY timestamp DESC LIMIT 10
                            """,
                            (component,),
                        )
                    else:
                        cursor.execute(
                            """
                            SELECT component, status, message, timestamp
                            FROM system_health
                            ORDER BY timestamp DESC LIMIT 50
                            """
                        )
                    results = cursor.fetchall()
            finally:
                conn.close()
            return [{"component": row[0], "status": row[1], "message": row[2], "timestamp": row[3]} for row in results]
        except Exception as e:
            logger.error(f"获取健康状态失败: {e}")
            return []

    def save_health_status(self, component: str, status: str, message: str = ""):
        with self.lock:
            conn = self._get_conn()
            try:
                self._execute(
                    conn,
                    "INSERT INTO system_health (component, status, message, timestamp) VALUES (%s, %s, %s, %s)",
                    (component, status, message, datetime.now()),
                )
                conn.commit()
            except Exception as e:
                logger.error(f"保存健康状态失败: {e}")
            finally:
                conn.close()

    def batch_save_futures_data(self, data_list: List[Dict[str, Any]]):
        try:
            for data in data_list:
                date_str = data.get("date", datetime.now().strftime("%Y-%m-%d"))
                sym = data.get("symbol")
                price = data.get("price", data.get("settle_price"))
                self.upsert_futures_daily(date=date_str, symbol=sym, settle_price=price)
            logger.info(f"批量保存期货数据: {len(data_list)} 条")
        except Exception as e:
            logger.error(f"批量保存期货数据失败: {e}")

    def batch_save_fund_prices(self, data_list: List[Dict[str, Any]]):
        try:
            for data in data_list:
                date_str = data.get("date", datetime.now().strftime("%Y-%m-%d"))
                self.save_fund_data(
                    date=date_str,
                    fund_code=data.get("code"),
                    price=data.get("price"),
                    nav=data.get("nav"),
                    premium=data.get("premium"),
                )
            logger.info(f"批量保存 fund 价格: {len(data_list)} 条")
        except Exception as e:
            logger.error(f"批量保存 fund 价格失败: {e}")

    def cleanup_old_data(self, days: int = 30):
        with self.lock:
            conn = self._get_conn()
            try:
                cutoff_date = (datetime.now() - pd.Timedelta(days=days)).strftime("%Y-%m-%d")
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM futures_daily WHERE date < %s", (cutoff_date,))
                    cursor.execute("DELETE FROM usa_etf_daily_prices WHERE date < %s", (cutoff_date,))
                    cursor.execute("DELETE FROM fund_data WHERE date < %s", (cutoff_date,))
                    cursor.execute("DELETE FROM system_health WHERE timestamp < %s", (cutoff_date,))
                conn.commit()
                logger.info(f"清理旧数据完成，保留最近 {days} 天")
            except Exception as e:
                logger.error(f"清理旧数据失败: {e}")
            finally:
                conn.close()

    def vacuum_database(self):
        engine = self.get_engine()
        try:
            with engine.begin() as conn:
                result = conn.execute(text("SHOW TABLES"))
                table_col = f"Tables_in_{self.database}"
                for row in result.mappings():
                    table_name = row[table_col]
                    conn.execute(text(f"OPTIMIZE TABLE `{table_name}`"))
            logger.info("MySQL OPTIMIZE TABLE 完成")
        except Exception as e:
            logger.error(f"MySQL 表优化失败: {e}")

import os

class Config:
    """Database configuration"""
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "algotrading")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_SCHEMA = "Orders"
    
    # Redis configuration
    REDIS_HOST     = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT     = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)
    REDIS_DB       = int(os.getenv("REDIS_DB", "0"))
    TOKENS_PATH    = os.getenv("TOKENS_PATH", "/home/algobaba/DATALORE/kite_data/")

    #COMMAND and LOG configuration
    SERVICE_COMMANDS = {
        "dataapi":{
            "COMMAND":"/mnt/DATA/LINUX_VENVS/FMV_SCALPER/bin/python /home/algobaba/DATALORE/TickEngine/start_tickengine.py",
            "LOGS_PATH":"/home/algobaba/DATALORE/logs/WSS_LOGS/",
            "LOG_NAME":"tickengine.log"
        },
        "orderprocessor":{
            "COMMAND":"/mnt/DATA/LINUX_VENVS/FMV_SCALPER/bin/python /home/algobaba/DATALORE/FairEdge/order_engine/run_order_engine.py",
            "LOGS_PATH":"/home/algobaba/DATALORE/logs/OMS_LOGS/",
            "LOG_NAME":"order_processor.log" 
        },
        "executionengine":{
            "COMMAND":"/mnt/DATA/LINUX_VENVS/FMV_SCALPER/bin/python /home/algobaba/DATALORE/FairEdge/execution_engine/execution_manager.py",
            "LOGS_PATH":"/home/algobaba/DATALORE/logs/EXE_LOGS/",
            "LOG_NAME":"execution_engine.log"
        }
    }
    
    
    @classmethod
    def get_connection_string(cls):
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"

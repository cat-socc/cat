import uvicorn
from fastapi import FastAPI
from routes.object_api import router as object_router
from core.dependencies import get_s3_boto, get_object_manager
import os
from common.logger import setup_crt_logging, setup_logging

# Set up logging level based on LOG_LEVEL environment variable
log_level = "ERROR"
setup_logging(log_level)

# 2. 启用 CRT 的详细日志
setup_crt_logging(level="DEBUG")  # 使用 "DEBUG" 查看最详细的日志
# setup_crt_logging(level="INFO")   # 使用 "INFO" 查看一般信息
# setup_crt_logging(level="WARNING") # 使用 "WARNING" 只查看警告和错误

app = FastAPI()

# 添加依赖
app.dependency_overrides[get_s3_boto] = get_s3_boto
app.dependency_overrides[get_object_manager] = get_object_manager

app.include_router(object_router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)

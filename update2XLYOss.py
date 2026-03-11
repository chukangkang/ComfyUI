# -*- coding: utf-8 -*-
# ============ 调用方法 ============
# 注意启动main.py时要使用无缓存模式 --cache-none，不然会导致相同参数直接走缓存，拿不到最终的数据
# from update2XLYOss import XLYUploadImageToOssNode


"""
1. main.py导入依赖
from update2XLYOss import XLYUploadImageToOssNode
2. 在
            q.task_done(item_id,
                        e.history_result,
                        status=execution.PromptQueue.ExecutionStatus(
                            status_str='success' if e.success else 'error',
                            completed=e.success,
                            messages=e.status_messages), process_item=remove_sensitive)
的后面添加入口
            try:
                if len(item) > 3 and isinstance(item[3], dict):
                    extra_data = item[3]
                    if extra_data.get('xly_callback_result_host'):
                        XLYUploadImageToOssNode().download_and_upload2_oss(item)
            except Exception as exception:
                logging.exception("上传 OSS 失败")
"""

"""
ComfyUI 自定义节点
上传 IMAGE 到阿里云 OSS（通过 getStsToken 获取临时凭证）

依赖:
pip install requests oss2
"""
import json
import oss2
import requests
import uuid
import yaml

# 导入自定义日志模块
from mylogger import logger, set_thread_local


def load_config():
    with open('/root/ComfyUI/xly-oss-upload-config.yaml', 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


config = load_config()

# ============ 配置项 ============
STS_TOKEN_API_URL = config.get("app", {}).get("stsTokenApiUrl")
OSS_ENDPOINT = config.get("app", {}).get("ossEndpoint")
COMFYUI_IMAGE_SERVER_URL = config.get("app", {}).get("comfyuiServerUrl")


class XLYUploadImageToOssNode:

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "image": ("IMAGE",),
                "user_id": ("STRING", {"default": "10001"}),
                "prompt_id": ("STRING", {"default": "prompt_id001"}),
                "request_id": ("STRING", {"default": "request_id_id001"}),
                "xly_callback_result_host": ("STRING", {"default": "https://dbsave.xingluan.cn"}),
                "xly_api_host": ("STRING", {"default": "https://api.xingluan.cn"})
            }
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("oss_url",)

    FUNCTION = "upload"
    CATEGORY = "XingLuan/OSS"

    def upload_bytes(self, image_bytes, user_id, prompt_id, filename, xly_api_host):
        # =============================
        # 1️⃣ 获取 STS 临时凭证 (逻辑复用)
        # =============================

        try:

            url = f"{xly_api_host}/xl-api/open/oss/getStsToken/{user_id}"
            logger.info(f"upload_bytes:url=[{url}]")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            raise ValueError(f"调用 getStsToken 接口失败: {response.text}")
        except Exception as e:
            raise ValueError(f"调用 getStsToken Exception: [{response}],{e}")
        token_data = response.json()

        access_key_id = token_data.get("accessKeyId")
        access_key_secret = token_data.get("accessKeySecret")
        security_token = token_data.get("securityToken")
        bucket_name = token_data.get("bucket")
        region = token_data.get("region")
        upload_dir = token_data.get("dir")

        if not all([access_key_id, access_key_secret, security_token,
                    bucket_name]):
            raise ValueError(f"getStsToken 返回数据不完整: {token_data}")

        # =============================
        # 2️⃣ 初始化 OSS 客户端
        # =============================
        if region:
            endpoint = f"https://oss-{region}.aliyuncs.com"
        else:
            endpoint = OSS_ENDPOINT
        auth = oss2.StsAuth(access_key_id, access_key_secret, security_token)
        bucket = oss2.Bucket(auth, endpoint, bucket_name)

        oss_key = f"{upload_dir}/{prompt_id}/{uuid.uuid4().hex}_{filename}"
        # =============================
        # 3️⃣ 直接上传字节流
        # =============================
        result = bucket.put_object(oss_key, image_bytes)
        if result.status != 200:
            raise Exception(f"上传到OSS失败, 状态码: {result.status}")

        # =============================
        # 4️⃣ 生成访问地址
        # =============================
        clean_endpoint = endpoint.replace("https://", "").replace("http://", "")
        oss_url = f"https://{bucket_name}.{clean_endpoint}/{oss_key}"

        logger.info(f"✅ 上传成功: {oss_url}")
        return (oss_url,)

    def get_comfy_history(self, prompt_id):
        """
        根据 prompt_id 从 ComfyUI 获取历史记录
        """
        if not prompt_id:
            return {}
        history_url = f"{COMFYUI_IMAGE_SERVER_URL}history/{prompt_id}"
        try:
            response = requests.get(history_url, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.info(
                    f"Warning: Failed to fetch history for {prompt_id}: {e}：{response.text}")
            return {}

    def extract_comfy_data(self, item):

        # 从 ComfyUI 的 item 和 e.result 中提取信息
        # 1. 提取 item 中的 prompt_id (index 1) 和额外信息 (index 3)
        prompt_id = item[1] if len(item) > 1 else None

        env = None
        user_id = None
        request_id = None
        if len(item) > 3 and isinstance(item[3], dict):
            extra_data = item[3]
            # 提取 env (优先使用 env1, 备选 custom.env)
            env = extra_data.get('env')
            user_id = extra_data.get('user_id')
            request_id = extra_data.get('request_id')
            xly_callback_result_host = extra_data.get('xly_callback_result_host')
            xly_api_host = extra_data.get('xly_api_host')

        # 调用 history 接口获取数据
        history_result = self.get_comfy_history(prompt_id)

        # 优先使用从接口拿到的数据，如果接口拿不到则使用传入的 result
        target_result = history_result
        if prompt_id and prompt_id in history_result:
            target_result = history_result[prompt_id]

        # 2. 提取 e.result 中的 filename 和 node_id
        # 遍历 outputs 寻找第一个包含图片的节点
        outputs = target_result.get("outputs", {})
        node_id = None
        filename = None
        subfolder = ""

        file_items = self.find_files_with_node(history_result)
        for item in file_items:
            print(item)
            node_id = item["node_id"]
            filename = item["filename"]
            subfolder = item["subfolder"]

        # 3. 提取 status 中的 execution_start 和 execution_success 时间戳
        status = target_result.get("status", {})
        messages = status.get("messages", [])
        execution_start = None
        execution_success = None

        for msg in messages:
            if isinstance(msg, list) and len(msg) >= 2:
                msg_type = msg[0]
                msg_data = msg[1]
                if msg_type == "execution_start":
                    execution_start = msg_data.get("timestamp")
                elif msg_type == "execution_success":
                    execution_success = msg_data.get("timestamp")

        return {
            "node_id": node_id,
            "filename": filename,
            "execution_start": execution_start,
            "execution_success": execution_success,
            "prompt_id": prompt_id,
            "env": env,
            "user_id": user_id,
            "request_id": request_id,
            "subfolder": subfolder,
            "xly_callback_result_host": xly_callback_result_host,
            "xly_api_host": xly_api_host
        }

    def find_files_with_node(self, history_result):
        """从 history_result 中提取所有文件信息，同时记录 prompt_id 和 node_id"""
        results = []
        for prompt_id, prompt_data in history_result.items():
            outputs = prompt_data.get("outputs", {})
            for node_id, node_output in outputs.items():
                # node_output 是类似 {"audio": [...]} 或 {"images": [...]} 的结构
                for output_type, file_list in node_output.items():
                    if isinstance(file_list, list):
                        for item in file_list:
                            if isinstance(item, dict) and "filename" in item and "subfolder" in item:
                                results.append({
                                    "prompt_id": prompt_id,
                                    "node_id": node_id,
                                    "output_type": output_type,  # "audio", "images" 等
                                    "filename": item["filename"],
                                    "subfolder": item["subfolder"],
                                })
        return results

    def report_task_result(self, task_id: str, oss_url: str = None,
            error_msg: str = None, request_id: str = "", node_id: str = "",
            execution_start: str = "0", execution_success: str = "0",
            data_source: str = "primary",
            xly_callback_result_host: str = "https://dbsave.xingluan.cn"):
        if oss_url:
            status = "completed"
            result_dict = {
                "request_id": request_id,
                "file_url": oss_url,
                "node_id": node_id,
                "start_timestamp": execution_start,
                "end_timestamp": execution_success,
            }
            result = json.dumps(result_dict)
            msg = ""
        else:
            status = "failed"
            result_dict = {
                "request_id": request_id,
                "node_id": node_id,
                "start_timestamp": execution_start,
                "end_timestamp": execution_success,
            }
            result = json.dumps(result_dict)
            msg = error_msg or "未知错误"

        payload = {
            "dataSource": data_source,
            "tableName": "model_task",
            "modelTaskRequest": {
                "taskId": task_id,
                "result": result,
                "status": status,
                "msg": msg
            }
        }

        try:
            # 使用 PreparedRequest 打印出最终发出的 HTTP body
            req = requests.Request('POST', xly_callback_result_host + "/dbsave/api/save/modelTask", json=payload)
            prepared = req.prepare()
            logger.info(
                    f"实际发出的 API 请求 Body:{prepared.body.decode('utf-8')}")

            resp = requests.Session().send(prepared, timeout=10)
            resp.raise_for_status()
            logger.info(
                    f"入库成功! 状态: {status}, taskId: {task_id},result: {result}")
        except requests.RequestException as e:
            logger.info(f"入库失败: {str(resp.text)}")

    def download_and_upload2_oss(self, item):
        """
        1. 调用 extract_comfy_data 获取 filename
        2. 从 ComfyUI API (127.0.0.1:12800) 获取图片
        3. 调用已有的 upload 方法上传到 OSS
        """
        # --- 1. 获取 ComfyUI 数据 ---
        logger.info(f"download_and_upload2_oss:{item}")
        comfy_data = self.extract_comfy_data(item)
        filename = comfy_data.get("filename")
        node_id = comfy_data.get("node_id")
        user_id = comfy_data.get("user_id")
        request_id = comfy_data.get("request_id")
        xly_api_host = comfy_data.get("xly_api_host")
        xly_callback_result_host = comfy_data.get("xly_callback_result_host")
        subfolder = comfy_data.get("subfolder")

        set_thread_local(request_id)

        env = comfy_data.get("env")
        execution_start = comfy_data.get("execution_start")
        execution_success = comfy_data.get("execution_success")

        # 优先使用提取出的 prompt_id，如果没有则尝试 extra_info 中的 taskId，最后默认
        prompt_id = comfy_data.get("prompt_id")

        if not filename:
            logger.info("错误: 未能在数据中提取到 filename")
            # 失败也记录一下库
            self.report_task_result(
                    task_id=prompt_id,
                    error_msg="未提取到文件名",
                    request_id=request_id,
                    node_id=node_id,
                    execution_start=execution_start,
                    execution_success=execution_success,
                    data_source=env,
                    xly_callback_result_host=xly_callback_result_host
            )
            return None
        # --- 2. 构建 API URL 并获取图片流 ---
        # 根据要求使用地址：127.0.0.1:12800
        view_url = f"{COMFYUI_IMAGE_SERVER_URL}view?filename={filename}&type=output&subfolder={subfolder}"

        try:
            logger.info(f"正在从 ComfyUI 下载图片: {filename}...")
            resp = requests.get(view_url, timeout=15)
            resp.raise_for_status()

            oss_url_tuple = self.upload_bytes(resp.content, user_id, prompt_id,
                                              filename, xly_api_host)
            # 在ComfyUI的规范中，自定义节点的FUNCTION（即你的upload方法）必须返回一个元组，即使里面只有一个值
            if oss_url_tuple and isinstance(oss_url_tuple, tuple):
                final_url = oss_url_tuple[0]
                logger.info(f"✅ 上传完成，OSS URL: {final_url}")

                # --- 5. 写库操作 ---
                logger.info(
                        f"正在写入任务结果到数据库 (taskId: {prompt_id},ossurl: {final_url})...")

                self.report_task_result(
                        task_id=prompt_id,
                        oss_url=final_url,
                        request_id=request_id,
                        node_id=node_id,
                        execution_start=execution_start,
                        execution_success=execution_success,
                        data_source=env,
                        xly_callback_result_host=xly_callback_result_host
                )

                return final_url

            return None
        except Exception as e:
            error_msg = str(e)
            logger.info(f"❌ 处理过程中发生错误: {error_msg}")
            # 发生异常时也尝试写库记录失败状态
            try:
                self.report_task_result(
                        task_id=prompt_id,
                        error_msg=error_msg,
                        request_id=request_id,
                        node_id=node_id,
                        data_source=env,
                        xly_callback_result_host=xly_callback_result_host
                )
            except:
                pass
            return None


# =============================
# ComfyUI 注册
# =============================

NODE_CLASS_MAPPINGS = {
    "XLYUploadImageToOssNode": XLYUploadImageToOssNode
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "XLYUploadImageToOssNode": "XingLuan Upload Image To OSS"
}

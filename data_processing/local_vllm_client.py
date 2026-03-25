#!/usr/bin/env python3
"""
阿里云百炼客户端（OpenAI Compatible API）

提供与历史代码兼容的两个函数：
- get_openai_response_for_video
- get_openai_response_for_text
"""

import json
import os
from urllib import error, request

DEFAULT_TEXT_MAX_TOKENS = 8192
DEFAULT_VIDEO_MAX_TOKENS = 16384
_LAST_REQUEST_META: dict = {}


def _build_base_url() -> str:
    base_url = os.environ.get(
        "DASHSCOPE_BASE_URL",
        "https://dashscope.aliyuncs.com/compatible-mode/v1",
    ).rstrip("/")
    if not base_url.endswith("/v1"):
        base_url = f"{base_url}/v1"
    return base_url


def _post_chat_completions(payload: dict) -> str:
    global _LAST_REQUEST_META
    base_url = _build_base_url()
    api_key = os.environ.get("DASHSCOPE_API_KEY", "")
    timeout = float(os.environ.get("DASHSCOPE_TIMEOUT_SECONDS", "600"))
    endpoint = f"{base_url}/chat/completions"

    if not api_key:
        raise RuntimeError("未设置 DASHSCOPE_API_KEY 环境变量")

    _LAST_REQUEST_META = {
        "endpoint": endpoint,
        "model": payload.get("model"),
        "max_tokens": payload.get("max_tokens"),
        "response_format": payload.get("response_format"),
    }

    req = request.Request(
        url=endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"DashScope HTTP 错误: {exc.code} {exc.reason}\n{detail}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"无法连接 DashScope 服务: {exc}") from exc

    try:
        parsed = json.loads(body)
        _LAST_REQUEST_META["request_id"] = parsed.get("request_id") or parsed.get("id")
        return parsed["choices"][0]["message"]["content"]
    except Exception as exc:
        raise RuntimeError(f"DashScope 返回格式异常: {body[:500]}") from exc


def get_last_request_meta() -> dict:
    """
    返回最近一次 chat/completions 请求的关键参数，便于在日志中核对。
    """
    return dict(_LAST_REQUEST_META)


def _resolve_video_url(input_video: str) -> str:
    """
    解析视频 URL：
    1) 优先使用 STSV_REMOTE_VIDEO_URL（便于本地文件 + 远程URL联动）
    2) 其次接受直接传入的 http/https URL
    3) 本地路径不再自动转 file://，云端模型无法访问本机文件
    """
    remote_url = os.environ.get("STSV_REMOTE_VIDEO_URL", "").strip()
    if remote_url:
        return remote_url

    candidate = (input_video or "").strip()
    if candidate.startswith("http://") or candidate.startswith("https://"):
        return candidate

    raise RuntimeError(
        "当前使用云端 API，需提供公网可访问的视频 URL。"
        "请设置 STSV_REMOTE_VIDEO_URL，或直接传入 https://... 视频地址。"
    )


def get_openai_response_for_text(
    system_prompt: str,
    user_prompt: str,
    model_name: str,
    temperature: float = 0.7,
    top_p: float = 0.95,
    max_tokens: int = DEFAULT_TEXT_MAX_TOKENS,
    **_,
) -> str:
    payload = {
        "model": model_name,
        "temperature": temperature,
        "top_p": top_p,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "response_format": {"type": "json_object"},
    }
    return _post_chat_completions(payload)


def get_openai_response_for_video(
    system_prompt: str,
    user_prompt: str,
    input_video: str,
    model_name: str,
    temperature: float = 0.7,
    top_p: float = 0.95,
    max_tokens: int = DEFAULT_VIDEO_MAX_TOKENS,
    **_,
) -> str:
    video_uri = _resolve_video_url(input_video)

    payload = {
        "model": model_name,
        "temperature": temperature,
        "top_p": top_p,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_prompt},
                    {"type": "video_url", "video_url": {"url": video_uri}},
                ],
            },
        ],
        "response_format": {"type": "json_object"},
    }
    return _post_chat_completions(payload)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
视频 -> 结构化训练样本 JSON 分析脚本（精简版）
仅负责：
- 调用视频理解模型
- 生成资产清单 / 情节 / 分镜结构化 JSON
- 训练 schema 下补齐时间与帧号字段
"""

import json
import os
import random
import time
import cv2
from pathlib import Path

from local_vllm_client import get_last_request_meta, get_openai_response_for_video

# ---------------------------
# Prompt 构建
# ---------------------------

def build_prompts():
    # 步骤2: 构建系统提示词
    system_prompt = """你是一名专业的影视导演分析师。你的任务是把输入视频转成“可训练分镜模型”的最小结构化数据。

训练目标是：输入【结构化剧本 + 资产清单】后生成【分镜】。
因此你必须输出资产、情节、分镜三层强关联数据，且字段严格最小化，并使用“beat 内嵌 shot”的结构。
你必须先理解剧情和台词，再做镜头拆解，禁止只看画面不看对白。

【输出目标】
你只能输出以下模块：
1) 顶层元信息：video_id, duration
2) 资产清单：characters, scenes, props
3) 情节：story_beats
4) 分镜：每个 story_beats 内嵌 shots（不要在顶层单独输出 shots）

【分析流程（必须遵守）】
阶段A：先按剧情推进切分 story_beats
- 每个 beat 只包含一个主导叙事点（一个小段剧情目标/冲突/结果）。
- beat 不能过长：默认建议 6~25 秒；通常不超过 45 秒（除非视频本身是长镜头且剧情单一）。
- 相邻 beat 要形成连续剧情链，避免把多个大事件塞进同一个 beat。

阶段B：在每个 beat 内做细粒度 shots 拆解
- 每个 beat 通常应有 3~8 个 shots；极简段落可 2 个，但必须有充分理由（例如稳定长镜头且信息变化少）。
- 不漏短镜头：凡是约 >=0.2 秒且具有信息价值的短暂镜头（反应镜头、插入镜头、转场瞬间、道具特写）都应单列 shot。
- 不硬凑镜头：只有当画面信息发生变化时才拆分（景别/机位/运镜/主体动作/视线对象/情绪焦点/说话人变化）。
- 同一 beat 下 shots 必须围绕同一叙事点，前后有因果或情绪递进，禁止无关镜头拼接。
- 同一 beat 内 shots 必须严格按时间顺序输出（start_time 递增），禁止先写后发生的镜头。

【核心约束】
1. 资产尽量全：尽量覆盖视频中出现的主要人物/场景/道具，宁可多列出轻量资产，也不要漏关键资产。
2. 资产描述短：每个资产描述要简短、可识别，不写长段落。
3. beats 要概括：每个 beat 是一个“可概括的主导叙事点”，必须包含核心动作、情感转折或关键信息点，防止剧本信息丢失。
4. 每个 shot 必须引用 scene_id 与资产 ID，并且放在对应 beat 的 shots 列表下。
5. shot 的 description 必须写全：角色、动作、情绪/目标、关键道具、环境信息都要出现。
6. description 中每个关键实体必须附带 ID 括号标注，例如：角色名(char_1)、道具名(prop_2)、场景名(scene_1)。
7. 每个 shot 必须包含 dialogue，若该镜头无可辨识台词则写空字符串 ""，禁止省略该字段。
8. dialogue 必须优先记录镜头内真实可听见的关键台词（简体中文转写，可适度精炼，不要瞎编）。
9. 每个 shot 必须包含 shot_purpose, shot_focus, composition 字段，这些字段对构图和叙事逻辑至关重要。
10. 每个 shot.description 必须更具体，建议 45~90 字，至少包含：景别、机位/角度、运镜、人物关系、关键动作、情绪变化、环境细节。
11. 即使该 shot 已填写 dialogue 字段，description 里也必须写出该镜头中人物说话/喊叫/低语等语言行为与关键台词信息（可简写，不要逐字重复整段）。
12. 每个 beat 内的 shots 列表顺序必须与时间一致：start_time 更早的 shot 必须排在前面。

【镜头字段约束】
- shot_type: wide | medium | close_up | extreme_close_up | long_shot | bird_eye_view 等（不限于此，可根据视频实际景别填写）
- camera_angle: eye_level | high | low | dutch_angle | over_the_shoulder 等（不限于此，可根据视频实际角度填写）
- camera_motion: static | pan | tilt | zoom_in | zoom_out | push | pull | track | arc_shot 等（不限于此，可根据视频实际运镜填写，去掉冗余的手持或摇臂描述）
- shot_purpose: establishing (交代空间) | emotion (情绪刻画) | action (动作表现) | reaction (反应) | insert (细节/道具) | transition (转场) 等
- shot_focus: 镜头的主体对象，如 "桑妮(char_1)的面部", "神经连接平板(prop_1)", "背景全息屏幕(prop_2)" 等
- composition: center | left | right | symmetrical | over_shoulder | rule_of_thirds | bottom | top
- lighting: natural | cinematic | high_key | low_key | neon | moody
- focus: deep_focus | shallow_focus | rack_focus

【ID 规范】
- character_id: char_1, char_2...
- scene_id: scene_1, scene_2...
- prop_id: prop_1, prop_2...
- beat_id: beat_1, beat_2...
- shot_id: shot_1, shot_2...
- order 从 1 开始递增

【语言与格式】
- JSON 字段名必须英文
- 自然语言内容必须简体中文
- 只输出 JSON，不得输出任何解释文字或 markdown
"""

    # 构建用户提示词
    user_prompt = """请分析整个视频，并严格按以下 schema 输出 JSON（不得增删字段）：
先做“剧情分段（story_beats）”，再做“beat 内细粒度分镜（shots）”；必须关注对白内容，不能忽略台词。

{
  "schema_version": "s2s_train_v1",
  "video_id": "string",
  "duration": 0,
  "characters": [
    {
      "character_id": "char_1",
      "name": "string",
      "appearance": "string",
      "clothing": "string",
      "traits": ["string"],
      "reference_frame": 0
    }
  ],
  "scenes": [
    {
      "scene_id": "scene_1",
      "name": "string",
      "location": "string",
      "time_of_day": "day | night",
      "environment": "string",
      "reference_frame": 0
    }
  ],
  "props": [
    {
      "prop_id": "prop_1",
      "name": "string",
      "description": "string",
      "owner_character_ids": ["char_1"],
      "reference_frame": 0
    }
  ],
  "story_beats": [
    {
      "beat_id": "beat_1",
      "order": 1,
      "scene_id": "scene_1",
      "characters": ["char_1"],
      "props": ["prop_1"],
      "summary": "详细描述该 beat 的剧情，包含核心动作、情感状态及关键信息点（防止信息丢失）。",
      "shots": [
        {
          "shot_id": "shot_1",
          "order": 1,
          "scene_id": "scene_1",
          "shot_type": "wide | medium | close_up | extreme_close_up | long_shot | bird_eye_view (或其它准确景别)",
          "camera_angle": "eye_level | high | low | dutch_angle | over_the_shoulder (或其它准确角度)",
          "camera_motion": "static | pan | tilt | zoom_in | zoom_out | push | pull | track | arc_shot (或其它准确运镜)",
          "shot_purpose": "establishing | emotion | action | reaction | insert | transition (或其它准确目的)",
          "shot_focus": "镜头的主体对象（如：角色名(char_1)的面部）",
          "composition": "center | left | right | symmetrical | over_shoulder | rule_of_thirds | bottom | top",
          "lighting": "natural | cinematic | high_key | low_key | neon | moody",
          "focus": "deep_focus | shallow_focus | rack_focus",
          "start_time": 0,
          "end_time": 0,
          "duration": 0,
          "dialogue": "示例：林夏(char_1)低声说“别回头，有人跟着我们”。无台词则填空字符串。",
          "description": "示例：中景平视镜头缓慢推进，女主林夏(char_1)一边急促呼吸一边把信封(prop_1)塞进外套口袋，神情紧张并反复回头确认是否有人跟踪；场景为昏暗走廊(scene_1)，墙面老旧且灯光闪烁。",
          "characters": ["char_1"],
          "props": ["prop_1"]
        }
      ]
    }
  ]
}

【强约束】
1) 资产数量尽量丰富，但每个描述简短清晰，避免长段文字。
2) 每个 beat 必须“概括一个主导情节点”，必须包含核心动作、情感转折或关键信息点，防止剧本信息丢失。
3) 每个 beat 默认建议 3~8 个 shots；极简段落可 2 个，但不能长期大面积只有 2 个。
4) 每个 shot 必须包含 shot_purpose, shot_focus, composition 字段，且其内容必须与画面内容及叙事逻辑严格对应。
5) 不要漏掉短暂镜头：约 >=0.2 秒且有叙事/情绪/信息价值的镜头要单独成 shot。
6) 不要硬凑镜头：同一 beat 下 shots 必须相关、连续、服务同一情节点；若画面信息无变化，不要强拆。
7) 不要在顶层输出 shots，shots 只能写在 story_beats[i].shots。
8) shots 的 order 在全片范围内连续递增（从 1 开始，不重复）。
9) 每个 shot 必须有 dialogue 字段：有台词就写关键原话（简体中文转写，可适度精炼）；无台词填 ""。
10) 每个 shot.description 必须写成细节化长描述（建议 2~3 句，约 45~90 字），至少覆盖：景别、机位/角度、运镜、人物关系、关键动作、情绪状态、场景氛围。
11) 即使 dialogue 字段已有台词，description 里仍必须体现“谁在说话/说了什么关键信息/说话时的状态与互动对象”。
12) description 中出现的角色/场景/道具都要带括号 ID 标注，如“林夏(char_1)”“信封(prop_1)”“走廊(scene_1)”。
13) 同一 beat 内 shots 必须严格按时间顺序排列，start_time 递增，且与 shots 的 order 一致；禁止时间顺序错乱。
14) 所有自然语言字段使用简体中文。
15) start_time / end_time / duration 都用秒（数字），且每个 shot 满足 start_time < end_time。
16) 每个 beat 建议时长 6~25 秒，通常不超过 45 秒；避免一个 beat 过长导致漏镜头。

只输出 JSON。
"""
    return system_prompt, user_prompt


# ---------------------------
# JSON 解析
# ---------------------------

def parse_json(text: str) -> dict:
    """
    解析 LLM 返回的 JSON 内容
    处理可能的 markdown 代码块格式和空响应
    """
    if not text or not text.strip():
        raise ValueError("LLM 返回的响应为空")
    
    # 尝试从字符串中提取 JSON
    # 处理可能的 markdown 代码块格式
    json_text = text.strip()
    
    if "```json" in json_text:
        json_text = json_text.split("```json")[1].split("```")[0].strip()
    elif "```" in json_text:
        # 处理 ``` 代码块（可能是其他语言标记）
        parts = json_text.split("```")
        if len(parts) >= 3:
            json_text = parts[1].split("\n", 1)[-1] if "\n" in parts[1] else parts[1]
            json_text = json_text.split("```")[0].strip()
    
    # 尝试找到第一个 { 和最后一个 }
    if "{" in json_text and "}" in json_text:
        start_idx = json_text.find("{")
        end_idx = json_text.rfind("}")
        json_text = json_text[start_idx:end_idx+1]
    
    try:
        return json.loads(json_text)
    except json.JSONDecodeError as e:
        print(f"❌ JSON 解析失败: {e}")
        print(f"📄 原始响应前500字符: {text[:500]}")
        print(f"📄 提取的 JSON 文本前500字符: {json_text[:500]}")
        raise ValueError(f"无法解析 LLM 返回的 JSON: {e}")


def infer_and_parse_with_retries(
    *,
    system_prompt: str,
    user_prompt: str,
    video_path: Path,
    model_name: str,
    temperature: float,
    top_p: float,
    max_retries: int,
    retry_temperature: float,
    retry_base_delay: float,
    retry_jitter: float,
) -> tuple[dict, str]:
    """
    调用模型并解析 JSON，失败时自动重试。
    为高并发场景增加指数退避和随机抖动，减少请求拥堵。
    """
    max_attempts = max_retries + 1
    last_error = None
    last_response = ""

    for attempt in range(1, max_attempts + 1):
        current_temperature = temperature if attempt == 1 else retry_temperature
        print(
            f"🔁 推理尝试 {attempt}/{max_attempts} "
            f"(temperature={current_temperature}, top_p={top_p})"
        )

        try:
            response = get_openai_response_for_video(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                input_video=str(video_path),
                model_name=model_name,
                temperature=current_temperature,
                top_p=top_p,
            )
            last_response = response or ""

            if not response or not response.strip():
                raise ValueError("LLM 返回的响应为空，请检查 API 调用是否成功")

            print(f"📝 收到响应，长度: {len(response)} 字符")
            result = parse_json(response)
            return result, response

        except Exception as e:
            last_error = e
            print(f"⚠️  第 {attempt}/{max_attempts} 次尝试失败: {e}")

            if last_response:
                print("📄 响应内容（前1000字符）:")
                print(last_response[:1000])

            if attempt >= max_attempts:
                break

            # 指数退避 + 随机抖动，降低并发重试冲突概率
            delay = retry_base_delay * (2 ** (attempt - 1)) + random.uniform(0, retry_jitter)
            print(f"⏳ {delay:.2f}s 后重试...")
            time.sleep(delay)

    raise RuntimeError(
        f"推理/解析在 {max_attempts} 次尝试后仍失败: {last_error}"
    ) from last_error


# ---------------------------
# 时间戳格式转换
# ---------------------------

def convert_minutes_seconds_to_seconds(value):
    """
    将可能是分:秒格式的数字转换为秒数
    例如：102 -> 1:02 = 62秒
    
    转换逻辑：只要是1开头的三位数，就按分:秒格式解析
    
    Args:
        value: 时间戳值（可能是数字）
        
    Returns:
        float: 转换后的秒数
    """
    if value is None:
        return value
    
    # 转换为浮点数
    num_value = float(value)
    
    # 如果是整数，检查是否可能是分:秒格式
    if num_value == int(num_value):
        # 转换为字符串，去掉小数点
        time_str = str(int(num_value))
        
        # 如果是三位数且以1开头，按分:秒格式解析
        if len(time_str) == 3 and time_str[0] == '1':
            # 第一位是分钟，后两位是秒
            minutes_part = int(time_str[0])
            seconds_part = int(time_str[1:])
            # 确保秒数部分 < 60
            if seconds_part < 60:
                minutes_seconds_value = minutes_part * 60 + seconds_part
                return minutes_seconds_value
    
    # 默认返回原值
    return num_value


# ---------------------------
# 视频信息获取
# ---------------------------

def get_video_fps(video_path: Path) -> float:
    """
    获取视频的帧率（FPS）
    
    Args:
        video_path: 视频文件路径
        
    Returns:
        float: 视频的帧率，如果失败返回 None
    """
    try:
        cap = cv2.VideoCapture(str(video_path))
        if not cap.isOpened():
            return None
        
        fps = cap.get(cv2.CAP_PROP_FPS)
        cap.release()
        return fps
    except Exception as e:
        print(f"⚠️  获取视频 FPS 失败: {e}")
        return None


def time_to_frame_number(time_seconds: float, fps: float) -> int:
    """
    将时间（秒）转换为帧号
    
    Args:
        time_seconds: 时间（秒）
        fps: 视频帧率
        
    Returns:
        int: 帧号（从0开始）
    """
    if fps is None or fps <= 0:
        return None
    return int(time_seconds * fps)


def ensure_valid_time_range(start_time: float, end_time: float, min_duration: float = 0.1) -> tuple:
    """
    确保时间范围有效：start < end 且差值至少为 min_duration
    
    Args:
        start_time: 开始时间（秒）
        end_time: 结束时间（秒）
        min_duration: 最小时长（秒），默认0.1秒
    
    Returns:
        (修正后的开始时间, 修正后的结束时间)
    """
    # 确保 start_time 和 end_time 是数字
    start_time = float(start_time) if start_time is not None else 0.0
    end_time = float(end_time) if end_time is not None else 0.0
    
    # 如果 start >= end，调整 end_time
    if start_time >= end_time:
        end_time = start_time + min_duration
    
    # 确保至少有一个最小时长
    if end_time - start_time < min_duration:
        end_time = start_time + min_duration
    
    return start_time, end_time


def add_frame_numbers_training_schema(result: dict, video_path: Path) -> dict:
    """
    为训练 schema（story_beats 内嵌 shots）计算首尾帧号
    """
    print("🎞️  开始计算训练schema帧号...")

    fps = get_video_fps(video_path)
    if fps is None:
        print("⚠️  无法获取视频 FPS，跳过训练schema帧号计算")
        return result

    print(f"📊 视频 FPS: {fps:.2f}")

    story_beats = result.get("story_beats", [])
    for beat in story_beats:
        shots = beat.get("shots", [])
        for shot in shots:
            start_time = shot.get("start_time")
            end_time = shot.get("end_time")

            try:
                start_time = convert_minutes_seconds_to_seconds(start_time) if start_time is not None else None
            except Exception:
                start_time = None
            try:
                end_time = convert_minutes_seconds_to_seconds(end_time) if end_time is not None else None
            except Exception:
                end_time = None

            if start_time is None or end_time is None:
                continue

            start_time, end_time = ensure_valid_time_range(start_time, end_time)
            shot["start_time"] = start_time
            shot["end_time"] = end_time
            shot["duration"] = end_time - start_time
            shot["start_frame"] = time_to_frame_number(start_time, fps)
            shot["end_frame"] = time_to_frame_number(end_time, fps)

    return result


# ---------------------------
# 核心分析函数
# ---------------------------

def analyze_video(
    video_path: Path,
    output_path: Path | None = None,
    model_name: str = "qwen3.5-plus",
):
    if not video_path.exists():
        raise FileNotFoundError(video_path)

    system_prompt, user_prompt = build_prompts()

    print(f"🎬 分析视频: {video_path.name}")
    print(f"🤖 使用模型: {model_name}")

    # 并发友好重试参数（可通过环境变量覆盖）
    # - STSV_LLM_MAX_RETRIES: 解析/推理失败后的重试次数（默认2，总尝试3次）
    # - STSV_LLM_RETRY_BASE_DELAY: 首次重试基础延迟秒数（默认1.0）
    # - STSV_LLM_RETRY_JITTER: 每次重试附加随机抖动秒数上限（默认1.0）
    # - STSV_LLM_RETRY_TEMPERATURE: 重试时温度（默认0.2，提高格式稳定性）
    max_retries = int(os.getenv("STSV_LLM_MAX_RETRIES", "2"))
    retry_base_delay = float(os.getenv("STSV_LLM_RETRY_BASE_DELAY", "1.0"))
    retry_jitter = float(os.getenv("STSV_LLM_RETRY_JITTER", "1.0"))
    retry_temperature = float(os.getenv("STSV_LLM_RETRY_TEMPERATURE", "0.5"))

    result, _response = infer_and_parse_with_retries(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        video_path=video_path,
        model_name=model_name,
        temperature=0.7,
        top_p=0.95,
        max_retries=max_retries,
        retry_temperature=retry_temperature,
        retry_base_delay=retry_base_delay,
        retry_jitter=retry_jitter,
    )

    # 统一保留视频路径，便于下游追溯源文件
    result["video_file_path"] = str(video_path)
    disable_frame_numbers = os.getenv("STSV_DISABLE_FRAME_NUMBER", "0") == "1"

    # 仅支持训练 schema；按需跳过首尾帧号
    if not disable_frame_numbers:
        result = add_frame_numbers_training_schema(result, video_path)

    # 输出路径
    if output_path is None:
        output_path = video_path.with_suffix(".storyboard.json")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    req_meta = get_last_request_meta()
    if req_meta:
        print(
            "🧾 请求参数: "
            f"model={req_meta.get('model')}, "
            f"max_tokens={req_meta.get('max_tokens')}, "
            f"response_format={req_meta.get('response_format')}, "
            f"request_id={req_meta.get('request_id')}"
        )

    print(f"✅ 分镜 JSON 已生成: {output_path}")
    print(f"📦 Scene 数量: {len(result.get('scenes', []))}")

    return result


# ---------------------------
# CLI
# ---------------------------

def main():
    import sys

    if len(sys.argv) < 2:
        print("用法: python analyze_video_to_json.py <video_path> [output_path]")
        sys.exit(1)

    video_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else None

    analyze_video(video_path, output_path)


if __name__ == "__main__":
    main()

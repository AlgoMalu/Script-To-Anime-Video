#!/usr/bin/env python3
"""
视频分割脚本
去掉视频前4分30秒和后2分钟，然后将中间部分每隔1分30秒切割成多个小视频
"""

import os
import subprocess
import sys
from pathlib import Path


def get_video_duration(video_path):
    """
    获取视频总时长（秒）
    
    Args:
        video_path: 视频文件路径
        
    Returns:
        视频时长（秒），如果获取失败返回None
    """
    cmd = [
        'ffprobe',
        '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        str(video_path)
    ]
    
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        duration = float(result.stdout.strip())
        return duration
    except (subprocess.CalledProcessError, ValueError) as e:
        print(f"错误: 无法获取视频时长: {e}")
        return None


def split_video_segment(input_video_path, start_time, duration, output_path):
    """
    切割视频片段
    
    Args:
        input_video_path: 输入视频路径
        start_time: 开始时间（秒）
        duration: 片段时长（秒）
        output_path: 输出文件路径
        
    Returns:
        成功返回True，失败返回False
    """
    # 使用参考代码中的方法：将 -ss 放在 -i 之后可以精确到帧级别，避免黑屏
    cmd = [
        'ffmpeg',
        '-i', str(input_video_path),
        '-ss', f"{start_time:.3f}",      # 开始时间（放在 -i 之后可精确到帧）
        '-t', f"{duration:.3f}",         # 片段时长
        '-c:v', 'libx264',               # 视频编码器
        '-preset', 'ultrafast',          # 快速编码预设
        '-crf', '23',                    # 质量参数
        '-c:a', 'copy',                  # 音频流复制
        '-y',                            # 覆盖已存在的文件
        str(output_path)
    ]
    
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"错误: ffmpeg 执行失败")
        print(f"错误信息: {e.stderr}")
        return False
    except FileNotFoundError:
        print(f"错误: 未找到 ffmpeg，请确保已安装 ffmpeg")
        return False


def extract_episode_number(filename):
    """
    从文件名中提取集数
    例如: "01.mp4" -> "01"
    
    Args:
        filename: 文件名
        
    Returns:
        集数字符串，如果无法提取则返回原文件名（不含扩展名）
    """
    stem = Path(filename).stem
    # 尝试提取开头的数字
    import re
    match = re.match(r'^(\d+)', stem)
    if match:
        return match.group(1)
    return stem


def split_video(input_video_path, output_base_dir=None):
    """
    处理视频：去掉前4分30秒和后2分钟，然后每隔1分30秒切割
    
    Args:
        input_video_path: 输入视频路径
        output_base_dir: 输出基础目录，如果为None则使用脚本所在目录下的divided_video
    """
    input_path = Path(input_video_path)
    
    if not input_path.exists():
        print(f"错误: 视频文件不存在: {input_video_path}")
        return False
    
    # 获取视频时长
    print(f"正在获取视频时长...")
    total_duration = get_video_duration(input_path)
    if total_duration is None:
        return False
    
    print(f"视频总时长: {total_duration:.2f} 秒 ({total_duration/60:.2f} 分钟)")
    
    # 参数设置
    skip_start = 4 * 60 + 30  # 前4分30秒 = 270秒
    skip_end = 2 * 60          # 后2分钟 = 120秒
    segment_duration = 1 * 60 + 30  # 每段1分30秒 = 90秒
    
    # 计算有效视频时长
    effective_duration = total_duration - skip_start - skip_end
    
    if effective_duration <= 0:
        print(f"错误: 视频时长不足，无法处理")
        print(f"需要至少 {skip_start + skip_end} 秒，实际只有 {total_duration:.2f} 秒")
        return False
    
    print(f"有效视频时长: {effective_duration:.2f} 秒 ({effective_duration/60:.2f} 分钟)")
    
    # 计算需要切割的段数
    num_segments = int(effective_duration / segment_duration)
    if effective_duration % segment_duration > 0:
        num_segments += 1  # 最后一段可能不足90秒
    
    print(f"将切割成 {num_segments} 段")
    
    # 确定输出目录
    if output_base_dir is None:
        script_dir = Path(__file__).parent
        output_base_dir = script_dir / "divided_video"
    else:
        output_base_dir = Path(output_base_dir)
    
    # 获取视频所在文件夹名称（如"辉夜大小姐"）
    video_parent_dir = input_path.parent.name
    episode_number = extract_episode_number(input_path.name)
    
    # 创建输出目录：divided_video/辉夜大小姐/
    output_dir = output_base_dir / video_parent_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n输出目录: {output_dir}")
    print(f"集数: {episode_number}")
    print(f"开始处理...\n")
    
    # 切割视频
    success_count = 0
    for i in range(num_segments):
        segment_start = skip_start + i * segment_duration
        # 最后一段可能不足90秒，使用剩余时长
        if i == num_segments - 1:
            segment_dur = effective_duration - i * segment_duration
        else:
            segment_dur = segment_duration
        
        # 生成输出文件名：例如 "01_part1.mp4", "01_part2.mp4"
        output_filename = f"{episode_number}_part{i+1}.mp4"
        output_path = output_dir / output_filename
        
        print(f"正在处理第 {i+1}/{num_segments} 段...")
        print(f"  开始时间: {segment_start:.2f} 秒 ({segment_start/60:.2f} 分钟)")
        print(f"  片段时长: {segment_dur:.2f} 秒 ({segment_dur/60:.2f} 分钟)")
        print(f"  输出文件: {output_path.name}")
        
        if split_video_segment(input_path, segment_start, segment_dur, output_path):
            print(f"  ✓ 成功\n")
            success_count += 1
        else:
            print(f"  ✗ 失败\n")
    
    print(f"\n处理完成！成功: {success_count}/{num_segments}")
    print(f"输出目录: {output_dir}")
    
    return success_count == num_segments


if __name__ == "__main__":
    # 默认处理指定的视频文件
    script_dir = Path(__file__).parent
    default_video = script_dir / "original_videos" / "辉夜大小姐" / "01.mp4"
    
    # 如果命令行提供了参数，使用命令行参数
    if len(sys.argv) > 1:
        input_video = Path(sys.argv[1])
    else:
        input_video = default_video
    
    if len(sys.argv) > 2:
        output_dir = Path(sys.argv[2])
    else:
        output_dir = None
    
    split_video(input_video, output_dir)


import os
import subprocess
import json
from collections import Counter

# ================= 配置区 =================
# 原始视频根目录
ROOT_DIR = "" 
# 处理后视频的根目录（保持目录结构）
OUTPUT_ROOT = "" 
MAX_EDGE = 640  
VIDEO_EXTENSIONS = ('.mp4', '.avi', '.mkv', '.mov', '.flv', '.wmv', '.webm', '.m4v', '.3gp', '.mpg', '.mpeg')
# ==========================================

def get_video_info(path):
    """获取视频宽高"""
    cmd = [
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries", "stream=width,height",
        "-of", "json", path
    ]
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        info = json.loads(result.stdout)
        if "streams" in info and len(info["streams"]) > 0:
            stream = info["streams"][0]
            return int(stream["width"]), int(stream["height"])
    except Exception:
        return None, None
    return None, None

def process_video(input_path, output_path):
    """一步到位：转换格式 + 缩放"""
    # 确保输出目录存在
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # -2 确保分辨率是偶数，这是 H.264 编码的强制要求
    scale_filter = f"scale='if(gt(iw,ih),{MAX_EDGE},-2)':'if(gt(ih,iw),{MAX_EDGE},-2)'"
    
    cmd = [
        "ffmpeg", "-y", "-i", input_path,
        "-vf", scale_filter,
        "-c:v", "libx264",
        "-preset", "fast",
        "-crf", "23",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac",
        "-b:a", "128k",
        "-movflags", "+faststart",
        output_path
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def main():
    if not os.path.exists(OUTPUT_ROOT):
        os.makedirs(OUTPUT_ROOT)

    # 1. 扫描所有目录，按目录分组处理
    dir_to_videos = {}
    for root, _, files in os.walk(ROOT_DIR):
        # 排除输出目录
        if os.path.abspath(root).startswith(os.path.abspath(OUTPUT_ROOT)):
            continue
            
        videos_in_dir = []
        for file in files:
            if file.lower().endswith(VIDEO_EXTENSIONS):
                if file in ["compress_video.py", "scan_videos.py"]:
                    continue
                videos_in_dir.append(file)
        
        if videos_in_dir:
            # 对当前文件夹内的文件进行自然排序
            videos_in_dir.sort()
            dir_to_videos[root] = videos_in_dir

    total_videos = sum(len(v) for v in dir_to_videos.values())
    print(f"🔍 找到视频总数: {total_videos}，分布在 {len(dir_to_videos)} 个文件夹中")
    
    global_idx = 0
    success_count = 0
    skip_count = 0

    # 2. 遍历每个文件夹进行处理
    for folder_path, files in dir_to_videos.items():
        rel_dir = os.path.relpath(folder_path, ROOT_DIR)
        print(f"\n📂 正在处理目录: {rel_dir}")
        
        for i, filename in enumerate(files):
            global_idx += 1
            input_path = os.path.join(folder_path, filename)
            
            # 生成新的文件名：001.mp4, 002.mp4 ...
            new_name = f"{i+1:03d}.mp4"
            output_path = os.path.join(OUTPUT_ROOT, rel_dir, new_name)

            print(f"[{global_idx}/{total_videos}] {filename} -> {new_name}")
            
            # 获取信息
            width, height = get_video_info(input_path)
            if not width or not height:
                print(f"  [Skip] 无法读取视频信息")
                continue

            # 检查是否已存在
            if os.path.exists(output_path):
                print(f"  [Skip] 目标文件已存在")
                skip_count += 1
                continue

            try:
                process_video(input_path, output_path)
                success_count += 1
            except Exception as e:
                print(f"  [Error] 处理失败: {e}")

    print(f"\n✅ 全部处理完成！")
    print(f"   - 成功: {success_count}")
    print(f"   - 跳过: {skip_count}")
    print(f"   - 根目录: {OUTPUT_ROOT}")

if __name__ == "__main__":
    main()

#!/bin/bash
# 批量执行脚本：遍历 data 目录下所有视频，并发执行 run_pipeline.sh
# 支持 nohup 执行，适合长时间运行
# 1. 可选压缩视频
# 2. 检查哪些视频已处理完成
# 3. 只处理未完成的视频

# 不使用 set -e，因为 xargs 中的任务失败不应该导致整个脚本退出

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 配置
SOURCE_VIDEO_DIR="${STSV_DATA_ROOT:-$SCRIPT_DIR/../data}"
PIPELINE_SCRIPT="$SCRIPT_DIR/run_pipeline.sh"
COMPRESS_SCRIPT="$SOURCE_VIDEO_DIR/compress_video.py"
MAX_CONCURRENT="${STSV_BATCH_MAX_CONCURRENT:-1}"  # 原视频级并发，默认1（避免并发叠加）
LOG_DIR="$SCRIPT_DIR/batch_logs"
PID_FILE="$SCRIPT_DIR/batch_pipeline.pid"
RESULT_DIR="$SCRIPT_DIR/result"
OSS_BUCKET="${STSV_OSS_BUCKET:-stsv-video}"
OSS_ENDPOINT="${STSV_OSS_ENDPOINT:-oss-cn-beijing.aliyuncs.com}"
OSS_ACCESS_KEY_ID="${STSV_OSS_ACCESS_KEY_ID:-${OSS_ACCESS_KEY_ID:-}}"
OSS_ACCESS_KEY_SECRET="${STSV_OSS_ACCESS_KEY_SECRET:-${OSS_ACCESS_KEY_SECRET:-}}"
OSSUTIL_BIN="${STSV_OSSUTIL_BIN:-ossutil}"

# 创建日志目录
mkdir -p "$LOG_DIR"

# 清理上次统计文件，避免累计
rm -f "$LOG_DIR/success.txt" "$LOG_DIR/failed.txt"

# 检查 pipeline 脚本是否存在
if [ ! -f "$PIPELINE_SCRIPT" ]; then
    echo -e "${RED}错误: 找不到 pipeline 脚本: $PIPELINE_SCRIPT${NC}"
    exit 1
fi

# 检查视频源目录是否存在
if [ ! -d "$SOURCE_VIDEO_DIR" ]; then
    echo -e "${RED}错误: 找不到视频目录: $SOURCE_VIDEO_DIR${NC}"
    exit 1
fi

# 校验 OSS 基础配置（提前失败，避免并发任务全部报错）
if [ -z "$OSS_BUCKET" ]; then
    echo -e "${RED}错误: 未设置 STSV_OSS_BUCKET${NC}"
    exit 1
fi
if [ -z "$OSS_ENDPOINT" ]; then
    echo -e "${RED}错误: 未设置 STSV_OSS_ENDPOINT${NC}"
    exit 1
fi
if [ -z "$OSS_ACCESS_KEY_ID" ] || [ -z "$OSS_ACCESS_KEY_SECRET" ]; then
    echo -e "${RED}错误: 未设置 OSS 访问凭证（STSV_OSS_ACCESS_KEY_ID / STSV_OSS_ACCESS_KEY_SECRET）${NC}"
    exit 1
fi
if ! command -v "$OSSUTIL_BIN" >/dev/null 2>&1 && ! python3 -c "import oss2" >/dev/null 2>&1; then
    echo -e "${RED}错误: 未找到可用上传工具。请安装 ossutil，或安装 python 包 oss2${NC}"
    exit 1
fi

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}批量视频处理脚本${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "${YELLOW}视频源目录: $SOURCE_VIDEO_DIR${NC}"
echo -e "${YELLOW}最大并发数: $MAX_CONCURRENT${NC}"
echo -e "${YELLOW}日志目录: $LOG_DIR${NC}"
echo -e "${YELLOW}OSS Bucket: $OSS_BUCKET${NC}"
echo -e "${YELLOW}OSS Endpoint: $OSS_ENDPOINT${NC}"
echo ""

# 提取输出子目录（按 SOURCE_VIDEO_DIR 相对路径保留层级，不包含文件名）
extract_output_subdir() {
    local video_path="$1"
    local source_root="$2"
    python3 - "$video_path" "$source_root" << 'PY'
import sys
from pathlib import Path

video_path = Path(sys.argv[1]).resolve()
source_root = Path(sys.argv[2]).resolve()
subdir = video_path.parent.name  # fallback

try:
    rel = video_path.relative_to(source_root)
    if len(rel.parts) >= 2:
        subdir = str(Path(*rel.parts[:-1]))
except Exception:
    pass

print(subdir)
PY
}

# ============================================
# 步骤0: 压缩视频（可选）
# ============================================
echo -e "${GREEN}[步骤0] 开始压缩视频...${NC}"
if [ -f "$COMPRESS_SCRIPT" ]; then
    echo -e "${YELLOW}执行压缩脚本: $COMPRESS_SCRIPT${NC}"
    python3 "$COMPRESS_SCRIPT"
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ 视频压缩完成${NC}"
    else
        echo -e "${YELLOW}警告: 视频压缩过程中出现错误，但继续执行${NC}"
    fi
else
    echo -e "${YELLOW}警告: 压缩脚本不存在，跳过压缩步骤: $COMPRESS_SCRIPT${NC}"
fi
echo ""

# ============================================
# 判断视频是否已处理完成（训练数据模式）
# ============================================
is_video_processed() {
    local video_file="$1"
    local output_subdir
    output_subdir="$(extract_output_subdir "$video_file" "$SOURCE_VIDEO_DIR")"
    local episode_name
    episode_name="$(basename "$video_file" .mp4)"

    # 如果视频文件名包含 _compressed，去掉后缀再匹配结果
    if [[ "$episode_name" == *_compressed ]]; then
        episode_name="${episode_name%_compressed}"
    fi

    # 只检查第一阶段 JSON 结果：result/video_to_shot_first/<data相对目录>/<集名>_part*_分镜分析.json
    local output_dir="$RESULT_DIR/video_to_shot_first/$output_subdir"
    shopt -s nullglob
    local matches=("$output_dir"/"${episode_name}_part"*_分镜分析.json)
    shopt -u nullglob

    if [ ${#matches[@]} -gt 0 ]; then
        return 0  # 已处理
    fi
    return 1  # 未处理
}

# ============================================
# 获取要处理的视频文件（优先使用压缩后的）
# ============================================
get_video_to_process() {
    local video_file="$1"
    local dir
    dir="$(dirname "$video_file")"
    local basename
    basename="$(basename "$video_file" .mp4)"
    local compressed_file="$dir/${basename}_compressed.mp4"

    # 如果存在压缩后的视频，优先使用压缩后的
    if [ -f "$compressed_file" ]; then
        echo "$compressed_file"
    else
        echo "$video_file"
    fi
}

# 查找所有视频文件（只查找原始视频，不包括压缩后的）
echo -e "${BLUE}正在搜索视频文件...${NC}"
ALL_VIDEO_FILES=()
while IFS= read -r -d '' file; do
    basename="$(basename "$file" .mp4)"
    if [[ "$basename" != *_compressed ]]; then
        ALL_VIDEO_FILES+=("$file")
    fi
done < <(find "$SOURCE_VIDEO_DIR" -type f -name "*.mp4" -print0 | sort -z)

TOTAL_VIDEOS=${#ALL_VIDEO_FILES[@]}

if [ "$TOTAL_VIDEOS" -eq 0 ]; then
    echo -e "${RED}错误: 未找到任何视频文件${NC}"
    exit 1
fi

echo -e "${GREEN}找到 $TOTAL_VIDEOS 个原始视频文件${NC}"
echo ""

# 过滤出未处理的视频
echo -e "${BLUE}正在检查哪些视频已处理完成...${NC}"
VIDEO_FILES=()
SKIPPED_COUNT=0

for video_file in "${ALL_VIDEO_FILES[@]}"; do
    rel_path="${video_file#$SOURCE_VIDEO_DIR/}"
    if is_video_processed "$video_file"; then
        echo -e "${GREEN}✓ 跳过已处理: $rel_path${NC}"
        ((SKIPPED_COUNT++))
    else
        video_to_process="$(get_video_to_process "$video_file")"
        VIDEO_FILES+=("$video_to_process")
    fi
done

PENDING_COUNT=${#VIDEO_FILES[@]}

echo ""
echo -e "${YELLOW}统计信息:${NC}"
echo -e "  总视频数: $TOTAL_VIDEOS"
echo -e "  已处理: $SKIPPED_COUNT"
echo -e "  待处理: $PENDING_COUNT"
echo ""

if [ "$PENDING_COUNT" -eq 0 ]; then
    echo -e "${GREEN}所有视频都已处理完成！${NC}"
    exit 0
fi

# 显示将要处理的视频列表
echo -e "${YELLOW}待处理视频列表:${NC}"
for i in "${!VIDEO_FILES[@]}"; do
    video="${VIDEO_FILES[$i]}"
    rel_path="${video#$SOURCE_VIDEO_DIR/}"
    echo -e "  $((i+1)). $rel_path"
done
echo ""

# 保存 PID
echo $$ > "$PID_FILE"
echo -e "${YELLOW}PID已保存到: $PID_FILE${NC}"
echo -e "${YELLOW}如需停止，请运行: kill \$(cat $PID_FILE)${NC}"
echo ""

# 主执行流程
echo -e "${GREEN}开始批量处理...${NC}"
echo ""

# 创建任务列表文件
TASK_LIST="$LOG_DIR/task_list.txt"
printf '%s\n' "${VIDEO_FILES[@]}" > "$TASK_LIST"

# 使用 xargs 控制并发执行
completed=0
failed=0

printf '%s\n' "${VIDEO_FILES[@]}" | xargs -n 1 -P $MAX_CONCURRENT -I {} bash -c '
    video_file="{}"
    script_dir="'"$SCRIPT_DIR"'"
    source_video_dir="'"$SOURCE_VIDEO_DIR"'"
    pipeline_script="'"$PIPELINE_SCRIPT"'"
    log_dir="'"$LOG_DIR"'"

    rel_path="${video_file#$source_video_dir/}"
    video_basename="$(basename "$video_file" .mp4)"
    log_file="$log_dir/${video_basename}_$(date +%Y%m%d_%H%M%S).log"

    cd "$script_dir"

    echo "[$(date +"%Y-%m-%d %H:%M:%S")] 开始处理: $rel_path" | tee "$log_file"
    echo "日志文件: $log_file" | tee -a "$log_file"

    if bash "$pipeline_script" "$video_file" >> "$log_file" 2>&1; then
        echo "[$(date +"%Y-%m-%d %H:%M:%S")] ✓ 完成: $rel_path" | tee -a "$log_file"
        echo "✓ $rel_path" >> "$log_dir/success.txt"
        exit 0
    else
        echo "[$(date +"%Y-%m-%d %H:%M:%S")] ✗ 失败: $rel_path" | tee -a "$log_file"
        echo "✗ $rel_path" >> "$log_dir/failed.txt"
        exit 1
    fi
'

# 统计结果
if [ -f "$LOG_DIR/success.txt" ]; then
    completed=$(wc -l < "$LOG_DIR/success.txt")
else
    completed=0
fi

if [ -f "$LOG_DIR/failed.txt" ]; then
    failed=$(wc -l < "$LOG_DIR/failed.txt")
else
    failed=0
fi

# 清理 PID 文件
rm -f "$PID_FILE"

# 输出总结
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}批量处理完成！${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "${YELLOW}总视频数: $TOTAL_VIDEOS${NC}"
echo -e "${YELLOW}已跳过: $SKIPPED_COUNT${NC}"
echo -e "${YELLOW}待处理: $PENDING_COUNT${NC}"
echo -e "${GREEN}成功: $completed${NC}"
echo -e "${RED}失败: $failed${NC}"
echo -e "${YELLOW}日志目录: $LOG_DIR${NC}"
echo ""

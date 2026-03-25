#!/bin/bash
# 一键执行脚本：
# 1) 视频分割
# 2) 分镜分析（训练 schema）
# 3) 兼容旧流程时可继续做质量评估

set -e  # 遇到错误立即退出

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
DATA_ROOT="${STSV_DATA_ROOT:-$SCRIPT_DIR/../data}"

# OSS 配置（用于云端 API 读取视频）
OSS_BUCKET="${STSV_OSS_BUCKET:-stsv-video}"
OSS_ENDPOINT="${STSV_OSS_ENDPOINT:-oss-cn-beijing.aliyuncs.com}"
OSS_PREFIX="${STSV_OSS_PREFIX:-}"
OSS_SCHEME="${STSV_OSS_URL_SCHEME:-https}"
OSS_ACCESS_KEY_ID="${STSV_OSS_ACCESS_KEY_ID:-${OSS_ACCESS_KEY_ID:-}}"
OSS_ACCESS_KEY_SECRET="${STSV_OSS_ACCESS_KEY_SECRET:-${OSS_ACCESS_KEY_SECRET:-}}"
OSSUTIL_BIN="${STSV_OSSUTIL_BIN:-ossutil}"
DELETE_DIVIDED_ON_SUCCESS="${STSV_DELETE_DIVIDED_ON_SUCCESS:-1}"
ANALYZE_MAX_CONCURRENT="${STSV_ANALYZE_MAX_CONCURRENT:-8}"
TMP_WORK_DIR="${TMPDIR:-/tmp}"
mkdir -p "$TMP_WORK_DIR"
export TMPDIR="$TMP_WORK_DIR"

# 上传能力探测（优先 ossutil，兜底 python oss2）
HAS_OSSUTIL=0
HAS_PYTHON_OSS2=0
if command -v "$OSSUTIL_BIN" >/dev/null 2>&1; then
    HAS_OSSUTIL=1
fi
if python3 -c "import oss2" >/dev/null 2>&1; then
    HAS_PYTHON_OSS2=1
fi

# 清理 OSS 前缀首尾斜杠，避免双斜杠
if [ -n "$OSS_PREFIX" ]; then
    OSS_PREFIX="${OSS_PREFIX#/}"
    OSS_PREFIX="${OSS_PREFIX%/}"
fi

validate_oss_config() {
    if [ -z "$OSS_BUCKET" ]; then
        echo -e "${RED}错误: 未设置 STSV_OSS_BUCKET${NC}"
        return 1
    fi
    if [ -z "$OSS_ENDPOINT" ]; then
        echo -e "${RED}错误: 未设置 STSV_OSS_ENDPOINT${NC}"
        return 1
    fi
    if [ -z "$OSS_ACCESS_KEY_ID" ] || [ -z "$OSS_ACCESS_KEY_SECRET" ]; then
        echo -e "${RED}错误: 未设置 OSS 访问凭证（STSV_OSS_ACCESS_KEY_ID / STSV_OSS_ACCESS_KEY_SECRET）${NC}"
        return 1
    fi
    if [ "$HAS_OSSUTIL" -ne 1 ] && [ "$HAS_PYTHON_OSS2" -ne 1 ]; then
        echo -e "${RED}错误: 未找到可用上传工具。请安装 ossutil，或安装 python 包 oss2${NC}"
        return 1
    fi
    return 0
}

build_oss_object_key() {
    local local_file="$1"
    local divided_root="$SCRIPT_DIR/divided_video"
    local abs_local_file
    local abs_divided_root
    abs_local_file="$(cd "$(dirname "$local_file")" && pwd)/$(basename "$local_file")"
    abs_divided_root="$(cd "$divided_root" && pwd)"

    if [[ "$abs_local_file" != "$abs_divided_root/"* ]]; then
        echo -e "${RED}错误: 分片路径不在 divided_video 下: $abs_local_file${NC}" >&2
        return 1
    fi

    local rel_path="${abs_local_file#$abs_divided_root/}"
    if [ -n "$OSS_PREFIX" ]; then
        echo "$OSS_PREFIX/$rel_path"
    else
        echo "$rel_path"
    fi
}

build_oss_public_url() {
    local object_key="$1"
    python3 - "$OSS_SCHEME" "$OSS_BUCKET" "$OSS_ENDPOINT" "$object_key" << 'PY'
import sys
from urllib.parse import quote

scheme, bucket, endpoint, object_key = sys.argv[1:5]
print(f"{scheme}://{bucket}.{endpoint}/{quote(object_key, safe='/')}")
PY
}

upload_with_ossutil() {
    local local_file="$1"
    local object_key="$2"
    local config_file
    config_file="$(mktemp)"

    cat > "$config_file" << EOF
[Credentials]
language=EN
endpoint=${OSS_ENDPOINT}
accessKeyID=${OSS_ACCESS_KEY_ID}
accessKeySecret=${OSS_ACCESS_KEY_SECRET}
EOF

    "$OSSUTIL_BIN" cp "$local_file" "oss://${OSS_BUCKET}/${object_key}" --config-file "$config_file" -f >/dev/null
    local cp_status=$?
    rm -f "$config_file"
    return $cp_status
}

upload_with_python_oss2() {
    local local_file="$1"
    local object_key="$2"
    python3 - "$local_file" "$object_key" "$OSS_BUCKET" "$OSS_ENDPOINT" "$OSS_ACCESS_KEY_ID" "$OSS_ACCESS_KEY_SECRET" << 'PY'
import sys
import oss2

local_file, object_key, bucket_name, endpoint, ak, sk = sys.argv[1:7]
auth = oss2.Auth(ak, sk)
bucket = oss2.Bucket(auth, f"https://{endpoint}", bucket_name)
bucket.put_object_from_file(object_key, local_file)
PY
}

upload_to_oss() {
    local local_file="$1"
    local object_key="$2"

    if [ "$HAS_OSSUTIL" -eq 1 ]; then
        upload_with_ossutil "$local_file" "$object_key"
        return $?
    fi

    if [ "$HAS_PYTHON_OSS2" -eq 1 ]; then
        upload_with_python_oss2 "$local_file" "$object_key"
        return $?
    fi

    return 1
}

cleanup_local_divided_videos() {
    if [ "$DELETE_DIVIDED_ON_SUCCESS" != "1" ]; then
        echo -e "${YELLOW}保留分片视频（STSV_DELETE_DIVIDED_ON_SUCCESS=$DELETE_DIVIDED_ON_SUCCESS）${NC}"
        return 0
    fi

    echo -e "${YELLOW}开始删除本地分片视频（保留原视频）...${NC}"
    mapfile -d '' TO_DELETE_FILES < <(
        find "$DIVIDED_VIDEO_DIR" -type f -name "${EPISODE_NAME}_part*.mp4" -print0 | sort -z -V
    )

    if [ ${#TO_DELETE_FILES[@]} -eq 0 ]; then
        echo -e "${YELLOW}未找到需要删除的本地分片视频${NC}"
        return 0
    fi

    for f in "${TO_DELETE_FILES[@]}"; do
        rm -f "$f"
    done

    echo -e "${GREEN}✓ 已删除 ${#TO_DELETE_FILES[@]} 个本地分片视频${NC}"
    return 0
}

# 检查参数
if [ $# -lt 1 ]; then
    echo -e "${RED}错误: 请提供输入视频路径${NC}"
    echo "用法: $0 <输入视频路径>"
    echo "示例: $0 original_videos/辉夜大小姐/01.mp4"
    exit 1
fi

INPUT_VIDEO="$1"

# 处理相对路径和绝对路径
if [[ "$INPUT_VIDEO" = /* ]]; then
    # 绝对路径
    INPUT_VIDEO_FULL="$INPUT_VIDEO"
else
    # 相对路径，相对于脚本目录
    INPUT_VIDEO_FULL="$SCRIPT_DIR/$INPUT_VIDEO"
fi

# 转换为绝对路径
INPUT_VIDEO_FULL="$(cd "$(dirname "$INPUT_VIDEO_FULL")" && pwd)/$(basename "$INPUT_VIDEO_FULL")"

# 检查输入视频是否存在
if [ ! -f "$INPUT_VIDEO_FULL" ]; then
    echo -e "${RED}错误: 视频文件不存在: $INPUT_VIDEO_FULL${NC}"
    exit 1
fi

if ! validate_oss_config; then
    exit 1
fi

# 提取输出子目录（按 DATA_ROOT 相对路径保留层级，不包含文件名）
extract_output_subdir() {
    local video_path="$1"
    local data_root="$2"
    python3 - "$video_path" "$data_root" << 'PY'
import sys
from pathlib import Path

video_path = Path(sys.argv[1]).resolve()
data_root = Path(sys.argv[2]).resolve()
subdir = video_path.parent.name

try:
    rel = video_path.relative_to(data_root)
    if len(rel.parts) >= 2:
        subdir = str(Path(*rel.parts[:-1]))
except Exception:
    pass

print(subdir)
PY
}

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}开始处理视频: $(basename "$INPUT_VIDEO_FULL")${NC}"
echo -e "${GREEN}========================================${NC}"

# 提取输出子目录与集名
OUTPUT_SUBDIR="$(extract_output_subdir "$INPUT_VIDEO_FULL" "$DATA_ROOT")"
EPISODE_NAME="$(basename "$INPUT_VIDEO_FULL" .mp4)"

echo -e "${YELLOW}输出子目录: $OUTPUT_SUBDIR${NC}"
echo -e "${YELLOW}集数: $EPISODE_NAME${NC}"
echo -e "${YELLOW}OSS Bucket: $OSS_BUCKET${NC}"
echo -e "${YELLOW}OSS Endpoint: $OSS_ENDPOINT${NC}"

# ============================================
# 步骤1: 分割视频
# ============================================
echo ""
echo -e "${GREEN}[步骤1/3] 开始分割视频...${NC}"
python3 "$SCRIPT_DIR/split_videos.py" "$INPUT_VIDEO_FULL"

if [ $? -ne 0 ]; then
    echo -e "${RED}错误: 视频分割失败${NC}"
    exit 1
fi

DIVIDED_VIDEO_DIR="$SCRIPT_DIR/divided_video/$(basename "$(dirname "$INPUT_VIDEO_FULL")")"
if [ ! -d "$DIVIDED_VIDEO_DIR" ]; then
    echo -e "${RED}错误: 分割后的视频目录不存在: $DIVIDED_VIDEO_DIR${NC}"
    exit 1
fi

echo -e "${GREEN}✓ 视频分割完成${NC}"

# ============================================
# 步骤2: 分析每个分割后的视频
# ============================================
echo ""
echo -e "${GREEN}[步骤2/3] 开始上传并分析分割后的视频...${NC}"

# 创建输出目录（按 data 相对路径保留季等中间层）
OUTPUT_FIRST_DIR="$SCRIPT_DIR/result/video_to_shot_first/$OUTPUT_SUBDIR"
mkdir -p "$OUTPUT_FIRST_DIR"

# 找到所有分割后的视频文件（按名称排序，保留空格路径）
mapfile -d '' VIDEO_FILES < <(
    find "$DIVIDED_VIDEO_DIR" -type f -name "${EPISODE_NAME}_part*.mp4" -print0 | sort -z -V
)

if [ ${#VIDEO_FILES[@]} -eq 0 ]; then
    echo -e "${RED}错误: 未找到分割后的视频文件${NC}"
    exit 1
fi

echo -e "${YELLOW}找到 ${#VIDEO_FILES[@]} 个分割后的视频${NC}"

# 存储所有生成的JSON文件路径
JSON_FILES=()
FAILED_UPLOADS=0
FAILED_ANALYSIS=0

# 存储上传成功后的分片与URL映射，后续统一分析
UPLOADED_VIDEO_FILES=()
UPLOADED_REMOTE_URLS=()

# 逐个上传分片（阶段A：仅上传，不推理）
for i in "${!VIDEO_FILES[@]}"; do
    VIDEO_FILE="${VIDEO_FILES[$i]}"

    echo -e "${YELLOW}正在上传 [$((i+1))/${#VIDEO_FILES[@]}] $(basename "$VIDEO_FILE")...${NC}"

    # 生成 OSS object key，确保与 divided_video 下相对路径一致
    OBJECT_KEY="$(build_oss_object_key "$VIDEO_FILE")"
    if [ $? -ne 0 ] || [ -z "$OBJECT_KEY" ]; then
        echo -e "${RED}警告: 生成 OSS key 失败，跳过: $VIDEO_FILE${NC}"
        FAILED_UPLOADS=$((FAILED_UPLOADS + 1))
        continue
    fi

    echo -e "${YELLOW}  上传到 OSS: ${OBJECT_KEY}${NC}"
    set +e
    upload_to_oss "$VIDEO_FILE" "$OBJECT_KEY"
    UPLOAD_RESULT=$?
    set -e
    if [ $UPLOAD_RESULT -ne 0 ]; then
        echo -e "${RED}警告: OSS 上传失败: $VIDEO_FILE${NC}"
        FAILED_UPLOADS=$((FAILED_UPLOADS + 1))
        continue
    fi

    REMOTE_VIDEO_URL="$(build_oss_public_url "$OBJECT_KEY")"
    UPLOADED_VIDEO_FILES+=("$VIDEO_FILE")
    UPLOADED_REMOTE_URLS+=("$REMOTE_VIDEO_URL")
    echo -e "${GREEN}✓ 上传完成: ${OBJECT_KEY}${NC}"
done

if [ ${#UPLOADED_VIDEO_FILES[@]} -eq 0 ]; then
    echo -e "${YELLOW}警告: 没有任何分片上传成功，跳过当前视频后续分析${NC}"
    cleanup_local_divided_videos
    exit 0
fi

if [ "$FAILED_UPLOADS" -gt 0 ]; then
    echo -e "${YELLOW}警告: 有 $FAILED_UPLOADS 个分片上传失败，已跳过失败分片，继续分析其余分片${NC}"
fi

ANALYZE_CONCURRENCY="$ANALYZE_MAX_CONCURRENT"
if [ ${#UPLOADED_VIDEO_FILES[@]} -lt "$ANALYZE_CONCURRENCY" ]; then
    ANALYZE_CONCURRENCY=${#UPLOADED_VIDEO_FILES[@]}
fi
if [ "$ANALYZE_CONCURRENCY" -lt 1 ]; then
    ANALYZE_CONCURRENCY=1
fi

echo -e "${GREEN}✓ 上传阶段结束，开始统一分析可用分片（并发=$ANALYZE_CONCURRENCY）...${NC}"

ANALYZE_TASKS_FILE="$(mktemp)"
ANALYZE_SUCCESS_FILE="$(mktemp)"
ANALYZE_FAIL_FILE="$(mktemp)"

for i in "${!UPLOADED_VIDEO_FILES[@]}"; do
    VIDEO_FILE="${UPLOADED_VIDEO_FILES[$i]}"
    REMOTE_VIDEO_URL="${UPLOADED_REMOTE_URLS[$i]}"
    VIDEO_BASENAME="$(basename "$VIDEO_FILE" .mp4)"
    OUTPUT_JSON="$OUTPUT_FIRST_DIR/${VIDEO_BASENAME}_分镜分析.json"
    printf '%s\0%s\0%s\0' "$VIDEO_FILE" "$REMOTE_VIDEO_URL" "$OUTPUT_JSON" >> "$ANALYZE_TASKS_FILE"
done

export SCRIPT_DIR ANALYZE_SUCCESS_FILE ANALYZE_FAIL_FILE
xargs -0 -n 3 -P "$ANALYZE_CONCURRENCY" bash -c '
video_file="$1"
remote_video_url="$2"
output_json="$3"
video_name="$(basename "$video_file")"
echo "正在分析: $video_name"
set +e
STSV_DISABLE_FRAME_NUMBER=1 STSV_REMOTE_VIDEO_URL="$remote_video_url" python3 "$SCRIPT_DIR/analyze_video_to_shots.py" "$video_file" "$output_json"
analyze_status=$?
set -e
if [ $analyze_status -eq 0 ] && [ -f "$output_json" ]; then
    echo "$output_json" >> "$ANALYZE_SUCCESS_FILE"
    echo "✓ 分析完成: $(basename "$output_json")"
else
    echo "$video_file" >> "$ANALYZE_FAIL_FILE"
    echo "警告: 视频分析失败: $video_file"
fi
' _ < "$ANALYZE_TASKS_FILE"

mapfile -t JSON_FILES < <(sort -u "$ANALYZE_SUCCESS_FILE")
FAILED_ANALYSIS=0
if [ -f "$ANALYZE_FAIL_FILE" ]; then
    FAILED_ANALYSIS=$(wc -l < "$ANALYZE_FAIL_FILE")
fi
rm -f "$ANALYZE_TASKS_FILE" "$ANALYZE_SUCCESS_FILE" "$ANALYZE_FAIL_FILE"

if [ ${#JSON_FILES[@]} -eq 0 ]; then
    echo -e "${YELLOW}警告: 没有成功生成任何JSON文件，跳过当前视频后续步骤${NC}"
    cleanup_local_divided_videos
    exit 0
fi

if [ "$FAILED_ANALYSIS" -gt 0 ]; then
    echo -e "${YELLOW}警告: 有 $FAILED_ANALYSIS 个分片分析失败，已跳过失败分片继续后续步骤${NC}"
fi

echo -e "${GREEN}✓ 分析阶段结束，成功生成 ${#JSON_FILES[@]} 个JSON文件${NC}"

# ============================================
# 完成（仅保留第一阶段输出）
# ============================================
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}处理完成（仅第一阶段）${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${YELLOW}输出文件:${NC}"
echo -e "  分割视频目录: $DIVIDED_VIDEO_DIR"
echo -e "  分镜分析目录: $OUTPUT_FIRST_DIR"
echo -e "  成功生成JSON数: ${#JSON_FILES[@]}"
echo ""

cleanup_local_divided_videos

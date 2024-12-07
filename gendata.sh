#!/bin/bash

# 设置目标文件夹
FOLDER_PATH="/home/shiwen/project/final/input"

# 检查文件夹是否存在，如果不存在则创建
if [ ! -d "$FOLDER_PATH" ]; then
  mkdir -p "$FOLDER_PATH"
  echo "创建文件夹 $FOLDER_PATH"
fi

# 生成128个文件，每个文件128MB
for i in $(seq 1 32); do
  FILE_NAME="file_${i}.bin"
  FILE_PATH="${FOLDER_PATH}/${FILE_NAME}"
  
  # 使用 dd 命令生成文件，4MB大，内容来自 /dev/urandom
  dd if=/dev/urandom of="$FILE_PATH" bs=4k count=1024 status=progress
  
  echo "生成文件: $FILE_PATH"
done

echo "所有文件生成完毕！"

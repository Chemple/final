import struct

# 检查文件中的 uint64_t 是否有序
def check_file_order(file_path):
    try:
        with open(file_path, "rb") as file:
            # 读取第一个 uint64_t 值
            prev_value = struct.unpack("Q", file.read(8))[0]
            position = 0  # 位置从0开始

            # 用来记录无序位置的列表
            unordered_positions = []

            # 逐个读取 uint64_t 数据
            while True:
                data = file.read(8)
                if len(data) < 8:
                    break  # 文件读取完毕

                # 解包 uint64_t 数据
                current_value = struct.unpack("Q", data)[0]
                position += 1

                # 检查当前值是否小于前一个值，表示无序
                if current_value < prev_value:
                    unordered_positions.append((position, prev_value, current_value))

                # 更新前一个值
                prev_value = current_value

            # 输出结果
            if not unordered_positions:
                print("文件中的 uint64_t 是有序的。")
            else:
                print(f"文件中的 uint64_t 不是有序的，共有{len(unordered_positions)}处")
                print("文件中的 uint64_t 不是有序的，以下是无序的位置和值：")
                for pos in unordered_positions:
                    print(f"位置 {pos[0]}: 当前值 = {pos[2]}, 前一个值 = {pos[1]}")

    except FileNotFoundError:
        print(f"文件 {file_path} 不存在。")
    except Exception as e:
        print(f"发生错误: {e}")


# 测试文件路径
file_path = "/home/shiwen/project/final/output/output1"  # 请根据需要修改文件路径
check_file_order(file_path)

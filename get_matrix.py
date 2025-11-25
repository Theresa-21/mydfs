import numpy as np
import sys

def create_sparse_matrices(rows1, cols1, cols2, in_file, out_file, max_value=10):
    total_size = rows1 * cols1 + cols1 * cols2
    non_zero_elements = total_size // 1000 * 6
    
    # 创建稀疏矩阵
    mat_A = np.zeros((rows1, cols1), dtype=int)
    mat_B = np.zeros((cols1, cols2), dtype=int)
    
    # 填充非零元素
    count = 0
    while count < non_zero_elements:
        r, c = np.random.randint(0, rows1), np.random.randint(0, cols1)
        if mat_A[r, c] == 0:
            mat_A[r, c] = np.random.randint(1, max_value)
            count += 1
        
        r, c = np.random.randint(0, cols1), np.random.randint(0, cols2)
        if mat_B[r, c] == 0:
            mat_B[r, c] = np.random.randint(1, max_value)
            count += 1

    # 保存输入文件
    with open(in_file, 'w') as f:
        f.write(f"{rows1} {cols1} {cols2}\n")
        for r in range(rows1):
            for c in range(cols1):
                if mat_A[r, c] != 0:
                    f.write(f"A,{r+1},{c+1},{mat_A[r, c]}\n")
        
        for r in range(cols1):
            for c in range(cols2):
                if mat_B[r, c] != 0:
                    f.write(f"B,{r+1},{c+1},{mat_B[r, c]}\n")

    # 计算并保存结果
    result = np.matmul(mat_A, mat_B)
    with open(out_file, 'w') as f:
        for r in range(rows1):
            for c in range(cols2):
                if result[r, c] != 0:
                    f.write(f"{r+1},{c+1},{result[r, c]}\n")
    
    print("Matrices generated successfully!")

if __name__ == "__main__":
    rA = int(sys.argv[1])
    cA = int(sys.argv[2])
    cB = int(sys.argv[3])
    input_file = sys.argv[4]
    output_file = sys.argv[5]
    if len(sys.argv) == 7:
        max_value = int(sys.argv[6])
        create_sparse_matrices(rA, cA, cB, input_file, output_file, max_value)
    else:
        create_sparse_matrices(rA, cA, cB, input_file, output_file)
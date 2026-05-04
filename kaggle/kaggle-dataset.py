import kagglehub
import shutil
import os

# Download to kagglehub cache
path = kagglehub.dataset_download("Cornell-University/arxiv")

# Copy to your desired destination
dest = "/Users/tomeravgil/Desktop/workspace/Flex-Research/kaggle/data"
os.makedirs(dest, exist_ok=True)
shutil.copytree(path, dest, dirs_exist_ok=True)

print("Files copied to:", dest)
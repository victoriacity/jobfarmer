
import sys, os, shutil

def backup(dir, name, files):
    bakdir = os.path.join(dir, name)
    os.makedirs(bakdir, exist_ok=True)
    for f in files:
        shutil.copy(os.path.join(dir, f), os.path.join(bakdir, f), follow_symlinks=False)

if __name__ == "__main__":
    backup(sys.argv[1], sys.argv[2], sys.argv[3:])
import subprocess
import shutil
import os
import sys

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DIST_DIR = os.path.join(ROOT_DIR, 'dist', 'VideoToMP3')
BUILD_DIR = os.path.join(ROOT_DIR, 'build')

def clean():
    for d in [DIST_DIR, BUILD_DIR]:
        if os.path.exists(d):
            shutil.rmtree(d)
    spec = os.path.join(BUILD_DIR, 'VideoToMP3.spec')
    if os.path.exists(spec):
        os.remove(spec)
    print('[clean] done')

def build_exe():
    cmd = [
        sys.executable, '-m', 'PyInstaller',
        '--onefile', '--windowed',
        '--name', 'VideoToMP3',
        '--distpath', os.path.join(ROOT_DIR, 'dist'),
        '--workpath', BUILD_DIR,
        '--specpath', BUILD_DIR,
        'app.pyw',
    ]
    print(f'[build] {" ".join(cmd)}')
    r = subprocess.run(cmd, cwd=ROOT_DIR)
    if r.returncode != 0:
        print('[build] FAILED')
        sys.exit(1)
    print('[build] exe done')

def assemble():
    os.makedirs(DIST_DIR, exist_ok=True)

    exe_src = os.path.join(ROOT_DIR, 'dist', 'VideoToMP3.exe')
    shutil.move(exe_src, os.path.join(DIST_DIR, 'VideoToMP3.exe'))

    for folder in ['bin', 'config']:
        src = os.path.join(ROOT_DIR, folder)
        dst = os.path.join(DIST_DIR, folder)
        if os.path.exists(dst):
            shutil.rmtree(dst)
        shutil.copytree(src, dst)

    for folder in ['output', 'logs']:
        os.makedirs(os.path.join(DIST_DIR, folder), exist_ok=True)

    print(f'[assemble] done -> {DIST_DIR}')

def make_zip():
    zip_path = os.path.join(ROOT_DIR, 'dist', 'VideoToMP3')
    shutil.make_archive(zip_path, 'zip', os.path.join(ROOT_DIR, 'dist'), 'VideoToMP3')
    print(f'[zip] done -> {zip_path}.zip')

if __name__ == '__main__':
    clean()
    build_exe()
    assemble()
    make_zip()
    print('\n  All done!')

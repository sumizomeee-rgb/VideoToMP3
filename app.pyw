import os
import sys
import json
import time
import threading
import subprocess
import queue
import re
from datetime import datetime
from pathlib import Path

# ============================================================
# 1. 常量与路径
# ============================================================
if getattr(sys, 'frozen', False):
    ROOT_DIR = os.path.dirname(sys.executable)
else:
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
URL_PATTERN = re.compile(r'https?://[^\s<>"\'}\]\)，。、！]+')
ILLEGAL_CHARS = re.compile(r'[\\/:*?"<>|]')
MAX_CONCURRENCY = 16

# ============================================================
# 2. 配置加载
# ============================================================
DEFAULT_CONFIG = {
    'outputDir': './output',
    'tools': {
        'ytdlpPath': './bin/yt-dlp.exe',
        'ffmpegPath': './bin/ffmpeg.exe',
    }
}

def load_config():
    config_path = os.path.join(ROOT_DIR, 'config', 'app.json')
    if os.path.isfile(config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
    else:
        cfg = json.loads(json.dumps(DEFAULT_CONFIG))
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)
    cfg['outputDir'] = os.path.normpath(os.path.join(ROOT_DIR, cfg.get('outputDir', './output')))
    cfg.setdefault('tools', {})
    cfg['tools']['ytdlpPath'] = os.path.normpath(os.path.join(ROOT_DIR, cfg['tools'].get('ytdlpPath', './bin/yt-dlp.exe')))
    cfg['tools']['ffmpegPath'] = os.path.normpath(os.path.join(ROOT_DIR, cfg['tools'].get('ffmpegPath', './bin/ffmpeg.exe')))
    return cfg

CONFIG = load_config()

os.makedirs(CONFIG['outputDir'], exist_ok=True)
os.makedirs(os.path.join(ROOT_DIR, 'logs'), exist_ok=True)

# ============================================================
# 3. 日志模块
# ============================================================
class Logger:
    def __init__(self):
        ts = datetime.now().strftime('%Y-%m-%d_%H%M%S')
        self.log_path = os.path.join(ROOT_DIR, 'logs', f'{ts}.log')
        self._file = open(self.log_path, 'a', encoding='utf-8')

    def log(self, level, msg):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f'[{ts}] [{level}] {msg}'
        self._file.write(line + '\n')
        self._file.flush()
        print(line)

    def close(self):
        self._file.close()

logger = Logger()

# ============================================================
# 4. 工具校验
# ============================================================
def check_ytdlp():
    p = CONFIG['tools']['ytdlpPath']
    if os.path.isfile(p):
        try:
            r = subprocess.run([p, '--version'], capture_output=True, timeout=10,
                               creationflags=subprocess.CREATE_NO_WINDOW)
            ver = r.stdout.decode('utf-8', errors='replace').strip()
            logger.log('INFO', f'yt-dlp found: v{ver}')
            return True
        except Exception as e:
            logger.log('WARN', f'yt-dlp check failed: {e}')
            return False
    logger.log('WARN', f'yt-dlp not found at {p}')
    return False

def check_ffmpeg():
    p = CONFIG['tools']['ffmpegPath']
    if os.path.isfile(p):
        try:
            r = subprocess.run([p, '-version'], capture_output=True, timeout=5,
                               creationflags=subprocess.CREATE_NO_WINDOW)
            ver = r.stdout.decode('utf-8', errors='replace').split('\n')[0]
            logger.log('INFO', f'ffmpeg found: {ver}')
            return True
        except Exception as e:
            logger.log('WARN', f'ffmpeg check failed: {e}')
            return False
    logger.log('WARN', f'ffmpeg not found at {p}')
    return False

YTDLP_OK = check_ytdlp()
FFMPEG_OK = check_ffmpeg()

# ============================================================
# 5. 工具函数
# ============================================================
def sanitize_filename(name):
    return ILLEGAL_CHARS.sub('_', name).strip()

def extract_urls(text):
    urls = URL_PATTERN.findall(text)
    cleaned = []
    seen = set()
    for url in urls:
        url = url.rstrip('.,;:!?)')
        if url not in seen:
            seen.add(url)
            cleaned.append(url)
    return cleaned

# ============================================================
# 6. 任务模型与队列
# ============================================================
class Task:
    _counter = 0

    def __init__(self, params):
        Task._counter += 1
        self.id = f'dl_{int(time.time()*1000)}_{Task._counter}'
        self.params = params
        self.status = 'queued'
        self.output_path = ''
        self.log_lines = []
        self.start_time = None
        self.elapsed = 0
        self.process = None
        self.title = ''
        self.fmt = params.get('format', '')

class TaskQueue:
    def __init__(self, max_concurrency):
        self.max_concurrency = max_concurrency
        self.waiting = []
        self.running = []
        self.finished = []
        self.lock = threading.Lock()
        self.ui_queue = queue.Queue()

    def submit(self, task, worker_fn):
        with self.lock:
            task._worker_fn = worker_fn
            if len(self.running) < self.max_concurrency:
                self._start(task)
            else:
                self.waiting.append(task)
                self.ui_queue.put(('queued', task))

    def _start(self, task):
        task.status = 'running'
        task.start_time = time.time()
        self.running.append(task)
        logger.log('INFO', f'Task {task.id} started')
        self.ui_queue.put(('started', task))
        t = threading.Thread(target=self._run, args=(task,), daemon=True)
        t.start()

    def _run(self, task):
        try:
            task._worker_fn(task, self)
        except Exception as e:
            task.status = 'failed'
            task.log_lines.append(f'ERROR: {e}')
            logger.log('ERROR', f'Task {task.id} exception: {e}')
        finally:
            self._on_done(task)

    def _on_done(self, task):
        with self.lock:
            if task in self.running:
                self.running.remove(task)
            task.elapsed = round(time.time() - (task.start_time or time.time()), 1)
            if task.status == 'running':
                task.status = 'completed'
            self.finished.append(task)
            if len(self.finished) > 100:
                self.finished.pop(0)
            logger.log('INFO' if task.status == 'completed' else 'ERROR',
                       f'Task {task.id} {task.status} in {task.elapsed}s')
            self.ui_queue.put(('done', task))
            if self.waiting:
                next_task = self.waiting.pop(0)
                self._start(next_task)

    def kill_all(self):
        with self.lock:
            for task in self.running:
                if task.process and task.process.poll() is None:
                    task.process.terminate()

task_queue = TaskQueue(MAX_CONCURRENCY)

# ============================================================
# 7. 子进程工作函数
# ============================================================
def worker_download(task, tq):
    url = task.params['url']
    fmt = task.params['format']
    output_dir = task.params.get('outputDir') or CONFIG['outputDir']
    os.makedirs(output_dir, exist_ok=True)

    ffmpeg_dir = os.path.dirname(CONFIG['tools']['ffmpegPath'])
    output_template = os.path.join(output_dir, '%(title)s.%(ext)s')

    args = [CONFIG['tools']['ytdlpPath']]

    if fmt in ('mp3', 'wav'):
        args += ['-x', '--audio-format', fmt, '--audio-quality', '0']
    elif fmt == 'mp4':
        args += ['--merge-output-format', 'mp4']

    args += [
        '--no-playlist',
        '--ffmpeg-location', ffmpeg_dir,
        '--encoding', 'utf-8',
        '--newline',
        '-o', output_template,
        url,
    ]
    logger.log('INFO', f'Task {task.id} cmd: {" ".join(args)}')

    proc = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NO_WINDOW
    )
    task.process = proc

    for line in proc.stdout:
        text = line.decode('utf-8', errors='replace').rstrip()
        if text:
            task.log_lines.append(text)
            if len(task.log_lines) > 200:
                task.log_lines.pop(0)
            if not task.title:
                if '[download] Destination:' in text:
                    dest = text.split('[download] Destination:')[-1].strip()
                    task.output_path = dest
                    task.title = os.path.splitext(os.path.basename(dest))[0]
                elif '[ExtractAudio] Destination:' in text:
                    dest = text.split('[ExtractAudio] Destination:')[-1].strip()
                    task.output_path = dest
                    task.title = os.path.splitext(os.path.basename(dest))[0]
            tq.ui_queue.put(('log', task))

    proc.wait()
    if proc.returncode != 0:
        task.status = 'failed'
        logger.log('ERROR', f'Task {task.id} yt-dlp exited with code {proc.returncode}')

# ============================================================
# 8. tkinter UI
# ============================================================
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title('VideoToMP3 - 视频转换下载器')
        self.geometry('720x620')
        self.minsize(640, 520)
        self.protocol('WM_DELETE_WINDOW', self._on_closing)

        self._build_input_frame()
        self._build_format_frame()
        self._build_output_frame()
        self._build_task_frame()
        self._build_log_frame()

        self._poll_ui_queue()
        logger.log('INFO', 'UI initialized')

    # ---- 链接输入区域 ----
    def _build_input_frame(self):
        frame = ttk.LabelFrame(self, text=' 粘贴链接 (支持多个，自动识别URL) ', padding=10)
        frame.pack(fill='x', padx=10, pady=(10, 5))

        text_frame = ttk.Frame(frame)
        text_frame.pack(fill='x')

        self.input_text = tk.Text(text_frame, height=4, font=('Consolas', 9), wrap='word')
        scroll = ttk.Scrollbar(text_frame, orient='vertical', command=self.input_text.yview)
        self.input_text.configure(yscrollcommand=scroll.set)
        self.input_text.pack(side='left', fill='both', expand=True)
        scroll.pack(side='right', fill='y')

        self._add_placeholder()
        self.input_text.bind('<FocusIn>', self._clear_placeholder)
        self.input_text.bind('<FocusOut>', self._restore_placeholder)

    def _add_placeholder(self):
        self.input_text.insert('1.0', '在这里粘贴视频链接，支持B站/YouTube等，可混合文字，自动提取URL')
        self.input_text.config(foreground='grey')
        self._has_placeholder = True

    def _clear_placeholder(self, event=None):
        if self._has_placeholder:
            self.input_text.delete('1.0', 'end')
            self.input_text.config(foreground='black')
            self._has_placeholder = False

    def _restore_placeholder(self, event=None):
        if not self.input_text.get('1.0', 'end').strip():
            self._add_placeholder()

    # ---- 格式选择区域 ----
    def _build_format_frame(self):
        frame = ttk.LabelFrame(self, text=' 输出格式 (至少选一个) ', padding=10)
        frame.pack(fill='x', padx=10, pady=5)

        self.fmt_mp3 = tk.BooleanVar()
        self.fmt_wav = tk.BooleanVar()
        self.fmt_mp4 = tk.BooleanVar()

        inner = ttk.Frame(frame)
        inner.pack(side='left')

        ttk.Checkbutton(inner, text='MP3', variable=self.fmt_mp3,
                        command=self._on_format_change).pack(side='left', padx=(0, 20))
        ttk.Checkbutton(inner, text='WAV', variable=self.fmt_wav,
                        command=self._on_format_change).pack(side='left', padx=(0, 20))
        ttk.Checkbutton(inner, text='MP4', variable=self.fmt_mp4,
                        command=self._on_format_change).pack(side='left', padx=(0, 20))

        self.dl_btn = ttk.Button(frame, text='开始下载', command=self._do_download, state='disabled')
        self.dl_btn.pack(side='right')

        if not YTDLP_OK or not FFMPEG_OK:
            missing = []
            if not YTDLP_OK: missing.append('yt-dlp')
            if not FFMPEG_OK: missing.append('ffmpeg')
            self.dl_btn.config(state='disabled')
            ttk.Label(frame, text=f'! {", ".join(missing)} 不可用', foreground='red').pack(side='right', padx=10)
            self._tools_ok = False
        else:
            self._tools_ok = True

    def _on_format_change(self):
        if not self._tools_ok:
            return
        any_checked = self.fmt_mp3.get() or self.fmt_wav.get() or self.fmt_mp4.get()
        self.dl_btn.config(state='normal' if any_checked else 'disabled')

    # ---- 输出目录区域 ----
    def _build_output_frame(self):
        frame = ttk.Frame(self, padding=(10, 0))
        frame.pack(fill='x', padx=0, pady=5)

        ttk.Label(frame, text='输出目录:').pack(side='left')
        self.outdir_entry = ttk.Entry(frame, width=50)
        self.outdir_entry.pack(side='left', padx=5, fill='x', expand=True)
        self.outdir_entry.insert(0, CONFIG['outputDir'])
        ttk.Button(frame, text='浏览', width=6, command=self._browse_outdir).pack(side='left', padx=(0, 5))
        ttk.Button(frame, text='打开', width=6, command=self._open_output_dir).pack(side='left')

    # ---- 任务列表区域 ----
    def _build_task_frame(self):
        frame = ttk.LabelFrame(self, text=' 任务列表 ', padding=10)
        frame.pack(fill='both', expand=True, padx=10, pady=5)

        cols = ('name', 'format', 'status', 'time')
        self.task_tree = ttk.Treeview(frame, columns=cols, show='headings', height=5)
        self.task_tree.heading('name', text='文件名')
        self.task_tree.heading('format', text='格式')
        self.task_tree.heading('status', text='状态')
        self.task_tree.heading('time', text='耗时')
        self.task_tree.column('name', width=360)
        self.task_tree.column('format', width=50, anchor='center')
        self.task_tree.column('status', width=80, anchor='center')
        self.task_tree.column('time', width=70, anchor='center')
        self.task_tree.pack(fill='both', expand=True)
        self.task_tree.bind('<<TreeviewSelect>>', self._on_task_select)

        self._task_items = {}

    # ---- 日志输出区域 ----
    def _build_log_frame(self):
        frame = ttk.LabelFrame(self, text=' 日志输出 ', padding=10)
        frame.pack(fill='both', expand=True, padx=10, pady=(5, 10))

        self.log_text = tk.Text(frame, height=5, state='disabled', wrap='word',
                                font=('Consolas', 9))
        scrollbar = ttk.Scrollbar(frame, orient='vertical', command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scrollbar.set)
        self.log_text.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')

        self._selected_task_id = None

    # ---- 事件处理 ----
    def _browse_outdir(self):
        d = filedialog.askdirectory(initialdir=self.outdir_entry.get())
        if d:
            self.outdir_entry.delete(0, 'end')
            self.outdir_entry.insert(0, d)

    def _open_output_dir(self):
        out = self.outdir_entry.get().strip() or CONFIG['outputDir']
        os.makedirs(out, exist_ok=True)
        os.startfile(out)

    def _do_download(self):
        if self._has_placeholder:
            messagebox.showerror('错误', '请粘贴视频链接')
            return

        text = self.input_text.get('1.0', 'end').strip()
        urls = extract_urls(text)
        if not urls:
            messagebox.showerror('错误', '未识别到有效链接，请检查输入')
            return

        formats = []
        if self.fmt_mp3.get(): formats.append('mp3')
        if self.fmt_wav.get(): formats.append('wav')
        if self.fmt_mp4.get(): formats.append('mp4')
        if not formats:
            messagebox.showerror('错误', '请至少选择一种输出格式')
            return

        output_dir = self.outdir_entry.get().strip() or None
        count = 0
        for url in urls:
            for fmt in formats:
                params = {'url': url, 'outputDir': output_dir, 'format': fmt}
                task = Task(params)
                task.title = url[:50]
                task.fmt = fmt
                task_queue.submit(task, worker_download)
                count += 1

        self.input_text.delete('1.0', 'end')
        self._has_placeholder = False
        logger.log('INFO', f'Submitted {count} tasks ({len(urls)} urls x {len(formats)} formats)')

    def _on_task_select(self, event):
        sel = self.task_tree.selection()
        if not sel:
            return
        item_id = sel[0]
        for tid, iid in self._task_items.items():
            if iid == item_id:
                self._selected_task_id = tid
                self._refresh_log(tid)
                break

    def _refresh_log(self, task_id):
        task = self._find_task(task_id)
        if not task:
            return
        self.log_text.config(state='normal')
        self.log_text.delete('1.0', 'end')
        self.log_text.insert('end', '\n'.join(task.log_lines[-100:]))
        self.log_text.see('end')
        self.log_text.config(state='disabled')

    def _find_task(self, task_id):
        for t in task_queue.running + task_queue.waiting + task_queue.finished:
            if t.id == task_id:
                return t
        return None

    # ---- UI 队列轮询 ----
    def _poll_ui_queue(self):
        try:
            while True:
                msg_type, task = task_queue.ui_queue.get_nowait()
                self._update_task_tree(task)
                if task.id == self._selected_task_id and msg_type == 'log':
                    self._refresh_log(task.id)
        except queue.Empty:
            pass
        self.after(300, self._poll_ui_queue)

    def _update_task_tree(self, task):
        status_map = {
            'queued': '等待中',
            'running': '下载中...',
            'completed': '完成',
            'failed': '失败',
        }
        display_name = task.title or task.params.get('url', '')[:50]
        elapsed = f'{task.elapsed}s' if task.elapsed else ''
        values = (display_name, task.fmt.upper(), status_map.get(task.status, task.status), elapsed)

        if task.id in self._task_items:
            self.task_tree.item(self._task_items[task.id], values=values)
        else:
            iid = self.task_tree.insert('', 0, values=values)
            self._task_items[task.id] = iid

    # ---- 窗口关闭 ----
    def _on_closing(self):
        if task_queue.running:
            if not messagebox.askokcancel('确认退出', f'还有 {len(task_queue.running)} 个任务运行中，确定退出？'):
                return
        task_queue.kill_all()
        logger.log('INFO', 'App closed')
        logger.close()
        self.destroy()

# ============================================================
# 9. CLI 测试接口
# ============================================================
def cli_download(urls, formats, output_dir=None):
    out = output_dir or CONFIG['outputDir']
    os.makedirs(out, exist_ok=True)
    done_event = threading.Event()
    results = []
    total = len(urls) * len(formats)
    lock = threading.Lock()

    original_on_done = task_queue._on_done
    def patched_on_done(task):
        original_on_done(task)
        with lock:
            results.append(task)
            status = 'OK' if task.status == 'completed' else 'FAIL'
            print(f'  [{len(results)}/{total}] {status} [{task.fmt}] {task.title or task.params["url"][:60]}')
            if task.log_lines:
                for line in task.log_lines[-3:]:
                    print(f'    {line}')
            if len(results) == total:
                done_event.set()
    task_queue._on_done = patched_on_done

    for url in urls:
        for fmt in formats:
            params = {'url': url, 'outputDir': out, 'format': fmt}
            task = Task(params)
            task.title = url[:60]
            task.fmt = fmt
            task_queue.submit(task, worker_download)

    done_event.wait()
    task_queue._on_done = original_on_done

    ok = sum(1 for t in results if t.status == 'completed')
    fail = total - ok
    print(f'\n  Done: {ok} succeeded, {fail} failed')
    print(f'  Output: {out}')
    return results

# ============================================================
# 10. 入口
# ============================================================
if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        fmt_arg = 'mp3'
        urls = []
        i = 2
        while i < len(sys.argv):
            if sys.argv[i] == '--format' and i + 1 < len(sys.argv):
                fmt_arg = sys.argv[i + 1]
                i += 2
            else:
                urls.append(sys.argv[i])
                i += 1
        if not urls:
            print('Usage: python app.pyw --test [--format mp3,wav,mp4] <url1> [url2] ...')
            sys.exit(1)
        formats = [f.strip() for f in fmt_arg.split(',')]
        print(f'\n  CLI test: {len(urls)} url(s), formats: {formats}\n')
        cli_download(urls, formats)
    else:
        logger.log('INFO', f'App started, ROOT_DIR={ROOT_DIR}')
        app = App()
        app.mainloop()

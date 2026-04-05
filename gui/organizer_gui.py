#!/usr/bin/env python3
from __future__ import annotations

import os
import queue
import re
import subprocess
import sys
import threading
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext

REPO = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = str(Path.home() / "folder-organizer-v2.yml")
DEFAULT_SERVICE_LABEL = "com.groqmcp.index-organizer.v2.service"


def resolve_python_executable() -> str:
    override = os.environ.get("ORGANIZER_PYTHON", "").strip()
    if override:
        candidate = Path(override).expanduser()
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)

    candidates = (
        Path.home() / "Documents" / ".venv" / "bin" / "python",
        Path("/opt/homebrew/opt/python@3.13/bin/python3.13"),
        Path("/opt/homebrew/bin/python3.13"),
        Path("/opt/homebrew/bin/python3"),
        Path("/usr/local/bin/python3.13"),
        Path("/usr/local/bin/python3"),
        Path("/usr/bin/python3"),
        Path(sys.executable),
    )
    for candidate in candidates:
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)
    return "python3"


def run_cmd(args: list[str], *, timeout: int = 180) -> str:
    try:
        result = subprocess.run(args, cwd=REPO, capture_output=True, text=True, timeout=timeout)
        output = (result.stdout or "") + ("\n" + result.stderr if result.stderr else "")
        return output.strip()
    except Exception as exc:
        return f"error: {exc}"


class OrganizerGUI:
    def __init__(self) -> None:
        self.root = tk.Tk()
        self.root.title("Folder Organizer GUI")
        self.root.geometry("1120x760")

        self.log_queue: queue.Queue[tuple[str, bool]] = queue.Queue()
        self.running = False
        self.action_buttons: list[tk.Button] = []

        self.python_var = tk.StringVar(value=resolve_python_executable())
        self.config_var = tk.StringVar(value=os.environ.get("ORGANIZER_CONFIG", DEFAULT_CONFIG))
        self.protection_var = tk.StringVar(value="strict")

        self._build_path_controls()
        self._build_toolbar()
        self._build_output_area()

        self.root.after(100, self._flush_log_queue)

    def _build_path_controls(self) -> None:
        panel = tk.Frame(self.root)
        panel.pack(fill=tk.X, padx=8, pady=(8, 4))

        tk.Label(panel, text="Python:").grid(row=0, column=0, sticky="w")
        tk.Entry(panel, textvariable=self.python_var).grid(row=0, column=1, sticky="ew", padx=(6, 6))
        tk.Button(panel, text="Auto", command=self.reset_python).grid(row=0, column=2, padx=(0, 10))

        tk.Label(panel, text="Config:").grid(row=1, column=0, sticky="w", pady=(6, 0))
        tk.Entry(panel, textvariable=self.config_var).grid(row=1, column=1, sticky="ew", padx=(6, 6), pady=(6, 0))
        tk.Button(panel, text="Browse", command=self.browse_config).grid(row=1, column=2, pady=(6, 0))

        panel.columnconfigure(1, weight=1)

    def _build_toolbar(self) -> None:
        toolbar = tk.Frame(self.root)
        toolbar.pack(fill=tk.X, padx=8, pady=(4, 8))

        self._add_action_button(toolbar, "Health", self.health)
        self._add_action_button(toolbar, "Status", self.status)
        self._add_action_button(toolbar, "Service Status", self.service_status)
        self._add_action_button(toolbar, "Plan", self.plan)
        self._add_action_button(toolbar, "Repair Tree", self.repair_tree)
        self._add_action_button(toolbar, "Tick Dry-Run", self.tick_dry_run)
        self._add_action_button(toolbar, "Tick Apply", self.tick_apply)

        tk.Label(toolbar, text="Protection:").pack(side=tk.LEFT, padx=(20, 4))
        option = tk.OptionMenu(toolbar, self.protection_var, "strict", "balanced", "audit")
        option.pack(side=tk.LEFT)
        self._add_action_button(toolbar, "Apply Protection", self.apply_protection)

        tk.Label(toolbar, text="|", padx=6).pack(side=tk.LEFT)
        tk.Button(toolbar, text="Open Config", command=self.open_config).pack(side=tk.LEFT, padx=4)
        tk.Button(toolbar, text="Save Log", command=self.save_log).pack(side=tk.LEFT, padx=4)
        tk.Button(toolbar, text="Clear", command=self.clear_log).pack(side=tk.LEFT, padx=4)

    def _build_output_area(self) -> None:
        self.output = scrolledtext.ScrolledText(self.root, wrap=tk.WORD)
        self.output.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

    def _add_action_button(self, parent: tk.Misc, text: str, command: callable) -> None:
        button = tk.Button(parent, text=text, command=command)
        button.pack(side=tk.LEFT, padx=4)
        self.action_buttons.append(button)

    def _set_busy(self, busy: bool) -> None:
        self.running = busy
        state = tk.DISABLED if busy else tk.NORMAL
        for button in self.action_buttons:
            button.configure(state=state)

    def _flush_log_queue(self) -> None:
        try:
            while True:
                message, done = self.log_queue.get_nowait()
                self.append(message)
                if done:
                    self._set_busy(False)
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self._flush_log_queue)

    def _python_executable(self) -> str:
        value = self.python_var.get().strip()
        if not value:
            value = resolve_python_executable()
            self.python_var.set(value)
        return str(Path(value).expanduser())

    def _config_path(self, *, must_exist: bool = True) -> Path | None:
        raw = self.config_var.get().strip()
        if not raw:
            messagebox.showerror("Error", "Config path is empty.")
            return None
        path = Path(raw).expanduser()
        if must_exist and not path.exists():
            messagebox.showerror("Error", f"Config not found: {path}")
            return None
        return path

    def _run_action(self, title: str, args: list[str], *, clip_lines: int | None = None, timeout: int = 180) -> None:
        if self.running:
            messagebox.showinfo("Busy", "Another command is currently running.")
            return

        def worker() -> None:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            header = f"[{timestamp}] {title}\n$ {' '.join(args)}"
            out = run_cmd(args, timeout=timeout)
            if clip_lines is not None:
                out = "\n".join(out.splitlines()[:clip_lines])
            payload = f"{header}\n{out}" if out else header
            self.log_queue.put((payload, True))

        self._set_busy(True)
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()

    def reset_python(self) -> None:
        self.python_var.set(resolve_python_executable())

    def browse_config(self) -> None:
        selected = filedialog.askopenfilename(
            title="Select organizer config",
            initialdir=str(Path.home()),
            filetypes=[("YAML", "*.yml *.yaml"), ("All files", "*.*")],
        )
        if selected:
            self.config_var.set(selected)

    def open_config(self) -> None:
        path = self._config_path(must_exist=True)
        if path is None:
            return
        subprocess.run(["open", str(path)], check=False)

    def save_log(self) -> None:
        target = filedialog.asksaveasfilename(
            title="Save organizer log",
            initialfile="organizer-gui.log",
            defaultextension=".log",
            filetypes=[("Log", "*.log"), ("Text", "*.txt"), ("All files", "*.*")],
        )
        if not target:
            return
        Path(target).write_text(self.output.get("1.0", tk.END), encoding="utf-8")
        self.append(f"Saved log: {target}")

    def clear_log(self) -> None:
        self.output.delete("1.0", tk.END)

    def append(self, text: str) -> None:
        self.output.insert(tk.END, text + "\n\n")
        self.output.see(tk.END)

    def health(self) -> None:
        self._run_action("Health", [self._python_executable(), "scripts/monitor.py", "--check-health"])

    def status(self) -> None:
        config_path = self._config_path(must_exist=True)
        if config_path is None:
            return
        self._run_action(
            "Status",
            [
                self._python_executable(),
                "index_organizer.py",
                "status",
                "--config",
                str(config_path),
                "--review-limit",
                "15",
            ],
        )

    def service_status(self) -> None:
        self._run_action("Service Status", [self._python_executable(), "organizer.py", "service-status"])

    def tick_dry_run(self) -> None:
        config_path = self._config_path(must_exist=True)
        if config_path is None:
            return
        self._run_action(
            "Service Tick (dry-run)",
            [
                self._python_executable(),
                "organizer.py",
                "service-tick",
                "--config",
                str(config_path),
            ],
            clip_lines=150,
        )

    def tick_apply(self) -> None:
        config_path = self._config_path(must_exist=True)
        if config_path is None:
            return
        if not messagebox.askyesno("Confirm", "Run service-tick with --apply?"):
            return
        self._run_action(
            "Service Tick (apply)",
            [
                self._python_executable(),
                "organizer.py",
                "service-tick",
                "--config",
                str(config_path),
                "--apply",
            ],
            clip_lines=200,
        )

    def plan(self) -> None:
        config_path = self._config_path(must_exist=True)
        if config_path is None:
            return
        self._run_action(
            "Plan",
            [self._python_executable(), "organizer.py", "plan", "--config", str(config_path)],
            clip_lines=150,
        )

    def repair_tree(self) -> None:
        config_path = self._config_path(must_exist=True)
        if config_path is None:
            return
        self._run_action(
            "Repair Tree",
            [self._python_executable(), "organizer.py", "repair-tree", "--config", str(config_path)],
            clip_lines=150,
        )

    def apply_protection(self) -> None:
        level = self.protection_var.get().strip().lower()
        config_path = self._config_path(must_exist=True)
        if config_path is None:
            return
        text = config_path.read_text(encoding="utf-8")
        if "protection_level:" in text:
            text = re.sub(r"(?m)^protection_level:\s*\w+\s*$", f"protection_level: {level}", text)
        else:
            text += f"\nprotection_level: {level}\n"
        config_path.write_text(text, encoding="utf-8")
        uid = subprocess.getoutput("id -u").strip()
        self._run_action(
            "Apply Protection",
            ["launchctl", "kickstart", "-k", f"gui/{uid}/{DEFAULT_SERVICE_LABEL}"],
        )
        self.append(f"protection_level={level} written to {config_path}")

    def run(self) -> None:
        self.root.mainloop()


if __name__ == "__main__":
    OrganizerGUI().run()
